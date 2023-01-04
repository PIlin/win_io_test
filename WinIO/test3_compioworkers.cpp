#include "tests.h"

#include <algorithm>
#include <functional>
#include <iostream>
#include <vector>

#include <atomic>
#include <thread>

namespace Test3
{

static const ULONG_PTR s_fileCompKey = 42;
static const ULONG_PTR s_stopCompKey = 28;

//struct SOnExit
//{
//	SOnExit(std::function<void()>&& cb) : callback(std::move(cb)) {}
//	~SOnExit() { callback(); }
//	std::function<void()> callback;
//};

struct SHandleCloser
{
	SHandleCloser(HANDLE h) : h(h) {}
	~SHandleCloser() { CloseHandle(h); }
	HANDLE h;
};


struct SBuffer final : public OVERLAPPED
{
	SBuffer(std::unique_ptr<char[]> p, size_t s)
		: pBuf(std::move(p))
		, bufSize(s)
	{
		memset(static_cast<OVERLAPPED*>(this), 0, sizeof(OVERLAPPED));
	}

	//SBuffer(const SBuffer&) = delete;
	//SBuffer& operator=(const SBuffer&) = delete;
	//SBuffer(SBuffer&&) = default;
	//SBuffer& operator=(SBuffer&&) = default;

	std::unique_ptr<char[]> pBuf;
	size_t bufSize;

	STimestamp pushTime;
};


struct SFileInfo
{
	std::atomic<fpos_t> off = { 0 };
	fpos_t fsizePos = 0;

	HANDLE hFile;
	HANDLE hComp;
	
	HANDLE hBufDoneEvent;
	std::atomic<int> activeBufCount = {};
};


static bool PushMoreRequests(SBuffer* pBuffers, size_t bufCount, SFileInfo& fi)
{
	PROF_FUNC();

	SBuffer* pBufEnd = pBuffers + bufCount;
	for (; pBuffers < pBufEnd; ++pBuffers)
	{
		SBuffer& buf = *pBuffers;

		fpos_t readSize = buf.bufSize;
		const fpos_t off = fi.off.fetch_add(readSize, std::memory_order_acq_rel);

		if (off >= fi.fsizePos)
		{
			return false;
		}
		readSize = std::min(readSize, fi.fsizePos - off);

		buf.Offset = static_cast<DWORD>(off);
		buf.OffsetHigh = static_cast<DWORD>(off >> (sizeof(buf.Offset)*8));

		buf.pushTime = STimestamp::now();

		const DWORD size = static_cast<DWORD>(readSize);
		const BOOL res = ReadFile(fi.hFile, buf.pBuf.get(), size, nullptr, &buf);
		const DWORD err = GetLastError();
		bool success = (res == TRUE || err == ERROR_IO_PENDING);
		if (!success)
		{
			std::cerr << "Failed to read file, err " << err <<  std::endl;
			exit(1);
			return false;
		}
	}
	return true;
}

struct SWorkerState
{
	SFileInfo* pFi = nullptr;
	int64 sum = 0;
	STimestamp popTime{};
	STimestamp sumTime{};
	STimestamp pushTime{};
	STimestamp readTime{};
	uint32 idx = 0;
};

struct alignas(128) SState
{
	static constexpr size_t alignment = 128;

	SState() = default;
	SState(SState&&) = default;
	SState& operator=(SState&&) = default;
	SState(const SState&) = delete;
	SState& operator=(const SState&) = delete;

	SWorkerState s;
	char padding[alignment - sizeof(s) % alignment];
};
static_assert(sizeof(SState) % SState::alignment == 0, "Broken alignment");

static void WorkerFunc(SWorkerState& state)
{
	SetThreadName(L"Worker_%u", state.idx);

	PROF_FUNC();

	uint64 s = 0;
	STimestamp popTime{};
	STimestamp sumTime{};
	STimestamp pushTime{};
	STimestamp readTime{};
	bool keepPushing = true;

	while (true)
	{
		DWORD transferred = 0;
		ULONG_PTR key = 0;
		OVERLAPPED* pOverlapped = nullptr;
		BOOL res;
		{
			PROF_REGION("GetQueuedCompletionStatus");
			STsRegion reg(popTime);
			res = GetQueuedCompletionStatus(state.pFi->hComp, &transferred, &key, &pOverlapped, INFINITE);
		}
		if (res == FALSE)
		{
			if (pOverlapped == nullptr)
			{
				const DWORD err = GetLastError();
				std::cerr << "Failed to get completion status, err " << err << std::endl;
				exit(3);
				return;
			}
			else
			{
				const DWORD err = GetLastError();
				std::cerr << "Failed to finsh reading file, err " << err << std::endl;
				exit(3);
				return;
			}
		}

		if (key == s_fileCompKey)
		{
			SBuffer& buf = *static_cast<SBuffer*>(pOverlapped);
			readTime += (STimestamp::now() - buf.pushTime);

			{
				PROF_REGION("sum");
				STsRegion reg(sumTime);
				s += sum(buf.pBuf.get(), transferred);
			}
			if (keepPushing)
			{
				STsRegion reg(pushTime);
				keepPushing = PushMoreRequests(&buf, 1, *state.pFi);
			}
			if (!keepPushing)
			{
				int val = state.pFi->activeBufCount.fetch_sub(1, std::memory_order_relaxed);
				if (val == 1)
				{
					SetEvent(state.pFi->hBufDoneEvent);
				}
			}
		}
		else if (key == s_stopCompKey)
		{
			break;
		}
		else
		{
			__debugbreak();
		}
	}

	state.sum = s;
	state.popTime = popTime;
	state.sumTime = sumTime;
	state.pushTime = pushTime;
	state.readTime = readTime;
}


}



void Test3_CompIOWorkers(const char* szFilename, const fpos_t fsizePos)
{
	PROF_FUNC();

	using namespace Test3;

	
	HANDLE hFile;
	{
		PROF_REGION("CreateFileA");
		hFile = CreateFileA(szFilename, GENERIC_READ, FILE_SHARE_READ, NULL, OPEN_EXISTING,
			FILE_ATTRIBUTE_NORMAL | FILE_FLAG_OVERLAPPED, NULL);
		if (hFile == INVALID_HANDLE_VALUE)
		{
			const DWORD err = GetLastError();
			std::cerr << "Failed to open file, err " << err << std::endl;
			return;
		}
	}
	SHandleCloser fileCloser(hFile);


	const uint32 hw = std::thread::hardware_concurrency();
	const int maxBuffers = 16;

	//DWORD compHw = 1;
	DWORD compHw = hw;
	HANDLE hComp;
	{
		PROF_REGION("CreateIoCompletionPort");
		hComp = CreateIoCompletionPort(hFile, NULL, s_fileCompKey, compHw);
		if (hComp == INVALID_HANDLE_VALUE)
		{
			const DWORD err = GetLastError();
			std::cerr << "Failed to create completion port, err " << err << std::endl;
			return;
		}
	}
	SHandleCloser compCloser(hComp);

	const size_t bufSize = 512 * 1024;

	std::vector<SBuffer> buffers;
	for (int i = 0; i < maxBuffers; ++i)
	{
		buffers.emplace_back(std::unique_ptr<char[]>(new char[bufSize]), bufSize);
	}

	const uint32 workerCount = std::max(hw, 2u) - 1;
	//const uint32 workerCount = 1;
	//const uint32 workerCount = hw;

	SFileInfo fi;
	fi.fsizePos = fsizePos;
	fi.hFile = hFile;
	fi.hComp = hComp;
	fi.hBufDoneEvent = CreateEvent(NULL, FALSE, FALSE, NULL);
	SHandleCloser bufDoneEventCloser(fi.hBufDoneEvent);

	std::vector<std::thread> workers;
	std::unique_ptr<SState[]> states(new SState[workerCount]);
	for (uint32 i = 0; i < workerCount; ++i)
	{
		states[i].s.idx = i;
		states[i].s.pFi = &fi;
		workers.emplace_back(std::thread(&WorkerFunc, std::ref(states[i].s)));
	}

	STimestamp sumTime{};
	STimestamp readTime{};
	//STimestamp freeBufferFetchTime{};
	STimestamp workPushTime{};
	STimestamp waitingForResultTime{};
	STimestamp stoppingTime{};

	auto startTime = ts::now();

	{
		STsRegion tsreg(workPushTime);
		fi.activeBufCount += (int)buffers.size();
		PushMoreRequests(buffers.data(), buffers.size(), fi);
	}

	{
		PROF_REGION("WaitForSingleObject hBufDoneEvent");
		STsRegion tsreg(waitingForResultTime);
		WaitForSingleObject(fi.hBufDoneEvent, INFINITE);
	}
	{
		PROF_REGION("PostQueuedCompletionStatus");
		STsRegion tsreg(stoppingTime);
		for (uint32 i = 0; i < workerCount; ++i)
			PostQueuedCompletionStatus(hComp, 0, s_stopCompKey, nullptr);

		for (auto& t : workers)
			t.join();
	}
	auto endTime = ts::now();

	uint64 sum = 0;
	for (SState* s = states.get(); s != states.get() + workerCount; ++s)
		sum += s->s.sum;

	const int64 expectedSum = 1226802104644;
	if (sum != expectedSum)
		__debugbreak();

	STimestamp workersPopTime{};
	STimestamp workersPushTime{};
	for (SState* s = states.get(); s != states.get() + workerCount; ++s)
	{
		workersPopTime += s->s.popTime;
		workersPushTime += s->s.pushTime;
		readTime += s->s.readTime;
		sumTime += s->s.sumTime;
	}

	std::cout << __FUNCTION__ << std::endl;
	std::cout << "Total took  " << ms((endTime - startTime).dur()) << " - MB/s " << MBsec(fsizePos, (endTime - startTime)) << std::endl;
	std::cout << "Read took   " << ms(readTime.dur()) << " - MB/s " << MBsec(fsizePos, readTime) << std::endl;
	std::cout << "Sum took    " << ms(sumTime.dur()) << " - MB/s " << MBsec(fsizePos, sumTime) << std::endl;
	std::cout << "Sum/wr took " << ms(sumTime.dur() / workerCount) << " - MB/s " << MBsec(fsizePos, sumTime.dur() / workerCount) << std::endl;
	//std::cout << "freeBufferFetchTime   " << ms(freeBufferFetchTime.dur()) << std::endl;
	std::cout << "workPushTime          " << ms(workPushTime.dur()) << std::endl;
	std::cout << "waitingForResultTime  " << ms(waitingForResultTime.dur()) << std::endl;
	std::cout << "stoppingTime          " << ms(stoppingTime.dur()) << std::endl;
	std::cout << "workersPopTime        " << ms(workersPopTime.dur()) << std::endl;
	std::cout << "workersPushTime       " << ms(workersPushTime.dur()) << std::endl;
	std::cout << "Sum is " << sum << std::endl;
	std::cout << "workerCount " << workerCount << std::endl;
	std::cout << std::endl;
}


