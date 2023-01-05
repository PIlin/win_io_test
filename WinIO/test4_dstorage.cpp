#include "tests.h"

#include <array>
#include <algorithm>
#include <functional>
#include <iostream>
#include <vector>
#include <filesystem>

#include <atomic>
#include <thread>

#include <dstorage.h>
#include <winrt/base.h>
#include <wrl/wrappers/corewrappers.h>

namespace Test4
{

using winrt::com_ptr;
using winrt::check_hresult;

static constexpr bool s_singleRequestThread = false;
static constexpr bool s_unbufferedIo = true;


struct SHandleCloser
{
	SHandleCloser() = default;
	SHandleCloser(HANDLE h) : h(h) {}
	~SHandleCloser() 
	{ 
		Close();
	}

	SHandleCloser(const SHandleCloser&) = delete;
	SHandleCloser& operator=(const SHandleCloser&) = delete;

	SHandleCloser(SHandleCloser&& o) noexcept
	{
		Close();
		std::swap(h, o.h);
	}
	SHandleCloser& operator=(SHandleCloser&& o) noexcept
	{
		Close();
		std::swap(h, o.h);
		return *this;
	}

	void Close()
	{
		if (h != INVALID_HANDLE_VALUE)
		{
			CloseHandle(h);
			h = INVALID_HANDLE_VALUE;
		}
	}

	HANDLE h = INVALID_HANDLE_VALUE;
};


struct AlignedArrDeleter
{
	void operator()(void* ptr)
	{
		_aligned_free(ptr);
	}
};
using AlignedUniquePtr = std::unique_ptr<char[], AlignedArrDeleter>;


struct SBuffer final 
{
	SBuffer(AlignedUniquePtr p, size_t s)
		: pBuf(std::move(p))
		, bufSize(s)
	{
		constexpr BOOL manualReset = FALSE;
		constexpr BOOL initialState = FALSE;
		m_event.Attach(CreateEventW(nullptr, manualReset, initialState, nullptr));

		if (!m_event.IsValid())
			std::abort();
	}

	AlignedUniquePtr pBuf;
	size_t bufSize;
	size_t readSize;

	STimestamp pushTime;
	Microsoft::WRL::Wrappers::Event m_event;
};


struct SFileInfo
{
	std::atomic<fpos_t> off = { 0 };
	fpos_t fsizePos = 0;

	IDStorageFile* pFile;
	IDStorageQueue1* pQueue;

	Microsoft::WRL::Wrappers::Event bufDoneEvent;
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
		buf.readSize = readSize;

		DSTORAGE_REQUEST r{};
		r.Options.SourceType = DSTORAGE_REQUEST_SOURCE_FILE;
		r.Options.DestinationType = DSTORAGE_REQUEST_DESTINATION_MEMORY;
		r.Options.CompressionFormat = DSTORAGE_COMPRESSION_FORMAT_NONE;
		r.Source.File.Source = fi.pFile;
		r.Source.File.Offset = off;
		r.Source.File.Size = (UINT32)readSize;
		r.Destination.Memory.Buffer = buf.pBuf.get();
		r.Destination.Memory.Size = (UINT32)readSize;
		r.UncompressedSize = (UINT32)readSize;
		r.CancellationTag = reinterpret_cast<uint64_t>(&buf);

		fi.pQueue->EnqueueRequest(&r);
		ResetEvent(buf.m_event.Get());
		fi.pQueue->EnqueueSetEvent(buf.m_event.Get());

		buf.pushTime = STimestamp::now();
	}

	fi.pQueue->Submit();

	return true;
}

struct SWorkerState
{
	SFileInfo* pFi = nullptr;
	SBuffer* pBuffers = nullptr;
	size_t buffersCount = 0;
	HANDLE stopEvent;
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

	const size_t buffersCount = state.buffersCount;
	const DWORD eventsCount = (DWORD)buffersCount + 1;
	std::array<HANDLE, MAXIMUM_WAIT_OBJECTS> events;
	for (size_t i = 0; i < buffersCount; ++i)
	{
		events[i] = state.pBuffers[i].m_event.Get();
	}
	assert(eventsCount < MAXIMUM_WAIT_OBJECTS);
	events[buffersCount] = state.stopEvent;

	while (true)
	{
		constexpr BOOL waitAll = FALSE;
		DWORD res = WaitForMultipleObjects(eventsCount, events.data(), waitAll, INFINITE);
		if (res == WAIT_FAILED)
		{
			const DWORD err = GetLastError();
			std::cerr << "Failed to get completion status, err " << err << std::endl;
			exit(3);
			return;
		}

		DWORD idx = res - WAIT_OBJECT_0;
		if (idx < buffersCount)
		{
			SBuffer& buf = state.pBuffers[idx];
			readTime += (STimestamp::now() - buf.pushTime);

			{
				PROF_REGION("sum");
				STsRegion reg(sumTime);
				s += sum(buf.pBuf.get(), buf.readSize);
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
					SetEvent(state.pFi->bufDoneEvent.Get());
				}
			}
		}
		else if (idx == buffersCount)
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



void Test4_DStorage(const char* szFilename, const fpos_t fsizePos)
{
	PROF_FUNC();

	using namespace Test4;

	std::wstring path = std::filesystem::path(szFilename).native();

	com_ptr<IDStorageFactory> factory;
	check_hresult(DStorageGetFactory(IID_PPV_ARGS(factory.put())));

	com_ptr<IDStorageFile> file;
	check_hresult(factory->OpenFile(path.c_str(), IID_PPV_ARGS(file.put())));


	BY_HANDLE_FILE_INFORMATION info{};
	check_hresult(file->GetFileInformation(&info));
	uint64 fileSize = (uint64(info.nFileSizeHigh) << (sizeof(info.nFileSizeHigh) * 8)) + uint64(info.nFileSizeLow);
	assert(fileSize == fsizePos);


	DSTORAGE_QUEUE_DESC queueDesc{};
	queueDesc.Capacity = DSTORAGE_MAX_QUEUE_CAPACITY;
	queueDesc.Priority = DSTORAGE_PRIORITY_NORMAL;
	queueDesc.SourceType = DSTORAGE_REQUEST_SOURCE_FILE;
	queueDesc.Device = nullptr;

	com_ptr<IDStorageQueue1> queue;
	check_hresult(factory->CreateQueue(&queueDesc, IID_PPV_ARGS(queue.put())));



	const uint32 hw = std::min< uint32>(std::thread::hardware_concurrency(), 32);
	const int maxBuffers = 32;

	const size_t bufSize = 512 * 1024;
	//const size_t bufSize = 4 * 1024 * 1024;
	const size_t bufAlignment = 4096;

	std::vector<SBuffer> buffers;
	for (int i = 0; i < maxBuffers; ++i)
	{
		buffers.emplace_back(AlignedUniquePtr((char*)_aligned_malloc(bufSize, bufAlignment)), bufSize);
	}

	const uint32 workerCount = std::max(hw, 2u) - 1;
	//const uint32 workerCount = 1;
	//const uint32 workerCount = hw;

	SFileInfo fi;
	fi.fsizePos = fsizePos;
	fi.pFile = file.get();
	fi.pQueue = queue.get();
	fi.bufDoneEvent.Attach(CreateEvent(NULL, FALSE, FALSE, NULL));
		
	Microsoft::WRL::Wrappers::Event stopEvent(CreateEvent(nullptr, TRUE, FALSE, nullptr));
	std::vector<std::thread> workers;
	std::unique_ptr<SState[]> states(new SState[workerCount]);
	for (uint32 i = 0; i < workerCount; ++i)
	{
		states[i].s.idx = i;
		states[i].s.pFi = &fi;
		states[i].s.pBuffers = buffers.data();
		states[i].s.buffersCount = buffers.size();
		states[i].s.stopEvent = stopEvent.Get();
		workers.emplace_back(std::thread(&WorkerFunc, std::ref(states[i].s)));
	}
	
	STimestamp sumTime{};
	STimestamp readTime{};
	//STimestamp freeBufferFetchTime{};
	STimestamp workPushTime{};
	STimestamp waitingForResultTime{};
	STimestamp stoppingTime{};

	auto startTime = ts::now();

	bool keepPushing = false;
	{
		STsRegion tsreg(workPushTime);
		fi.activeBufCount += (int)buffers.size();
		keepPushing = PushMoreRequests(buffers.data(), buffers.size(), fi);
	}

	{
		PROF_REGION("WaitForSingleObject hBufDoneEvent");
		STsRegion tsreg(waitingForResultTime);
		WaitForSingleObject(fi.bufDoneEvent.Get(), INFINITE);
	}


	{
		PROF_REGION("PostQueuedCompletionStatus");
		STsRegion tsreg(stoppingTime);
		SetEvent(stopEvent.Get());

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


