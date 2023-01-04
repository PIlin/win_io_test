#include "tests.h"

#include <array>
#include <iostream>
#include <vector>

#include <atomic>
#include <mutex>
#include <thread>

namespace Test2
{
	struct SWork
	{
		const char* pBuf = nullptr;
		size_t size = 0;
	};

	constexpr int maxBuffers = 16;
	struct SWorkerState
	{
	private:
		static_assert(maxBuffers < 32, "");

		class CQueuesAtm
		{
		public:
			bool TryPushToInQueue(const SWork& work) { return TryPushToQueue(inQueueSlots, bufInQueue, work); }
			bool TryPushToOutQueue(const SWork& work) { return TryPushToQueue(outQueueSlots, bufOutQueue, work); }
			bool TryPopFromInQueue(SWork& work) { return TryPopFromQueue(inQueueSlots, bufInQueue, work); }
			bool TryPopFromOutQueue(SWork& work) { return TryPopFromQueue(outQueueSlots, bufOutQueue, work); }

			void Stop() {}

		private:
			static uint32 IntegerLog2(uint32 v)
			{
				if (!v) return uint32(~0UL);
				unsigned long result;
				_BitScanReverse(&result, v);
				return uint32(result);
			}
			static uint8 BitIndex(uint32 v) { return uint8(IntegerLog2(v)); }

			using BufferQueue = std::array<SWork, maxBuffers>;

			bool TryPushToQueue(std::atomic<uint32>& slotsAtm, BufferQueue& queue, const SWork& work)
			{
				uint32 cur = slotsAtm.load(std::memory_order_relaxed);
				constexpr uint32 maxMask = (1 << maxBuffers) - 1;
				if (cur >= maxMask)
					return false;

				uint32 unsetSlots = (~cur) & maxMask;
				uint8 unsetIdx = BitIndex(unsetSlots);

				queue[unsetIdx] = work;

				uint32 newSlots;
				do {
					newSlots = cur | (1 << unsetIdx);
				} while (!slotsAtm.compare_exchange_strong(cur, newSlots, std::memory_order_release, std::memory_order_relaxed));
				return true;
			}

			bool TryPopFromQueue(std::atomic<uint32>& slotsAtm, BufferQueue& queue, SWork& out)
			{
				uint32 cur = slotsAtm.load(std::memory_order_acquire);
				if (cur == 0)
					return false;
				uint8 idx = BitIndex(cur);
				out = queue[idx];

				queue[idx] = SWork();

				uint32 mask = ~(1 << idx);

				uint32 newSlots;
				do {
					newSlots = cur & mask;
				} while (!slotsAtm.compare_exchange_strong(cur, newSlots, std::memory_order_release, std::memory_order_relaxed));
				//} while (!slotsAtm.compare_exchange_strong(cur, newSlots, std::memory_order_relaxed));
				return true;
			}

			std::atomic<uint32> inQueueSlots = 0;
			std::atomic<uint32> outQueueSlots = 0;
			BufferQueue bufInQueue = {};
			BufferQueue bufOutQueue = {};
		};

		class CQueuesMtx
		{
			using BufferQueue = std::vector<SWork>;

		public:
			CQueuesMtx()
			{
				bufInQueue.reserve(maxBuffers);
				bufOutQueue.reserve(maxBuffers);
			}

			void Stop()
			{
				{
					std::scoped_lock lock(mtx);
					stop = true;
				}
				condIn.notify_one();
				condOut.notify_one();
			}

			bool TryPushToInQueue(const SWork& work) { return TryPushToQueue(condIn, bufInQueue, work); }
			bool TryPushToOutQueue(const SWork& work) { return TryPushToQueue(condOut, bufOutQueue, work); }
			bool TryPopFromInQueue(SWork& work) { return TryPopFromQueue(condIn, bufInQueue, work); }
			bool TryPopFromOutQueue(SWork& work) { return TryPopFromQueue(condOut, bufOutQueue, work); }
		private:
			bool TryPushToQueue(std::condition_variable& cond, BufferQueue& queue, const SWork& work)
			{
				{
					std::scoped_lock lock(mtx);
					queue.push_back(work);
				}
				cond.notify_one();
				return true;
			}

			bool TryPopFromQueue(std::condition_variable& cond, BufferQueue& queue, SWork& out)
			{
				std::unique_lock lock(mtx);
				cond.wait(lock, [&]() { return !queue.empty() || stop; });

				if (!queue.empty())
				{
					out = queue.back();
					queue.pop_back();
					return true;
				}
				else
				{
					return false;
				}
			}


			std::mutex mtx;
			std::condition_variable condIn;
			std::condition_variable condOut;
			BufferQueue bufInQueue;
			BufferQueue bufOutQueue;
			bool stop = false;
		};


	public:

		SWorkerState() = default;
		SWorkerState(SWorkerState&&) = default;
		SWorkerState& operator=(SWorkerState&&) = default;
		SWorkerState(const SWorkerState&) = delete;
		SWorkerState& operator=(const SWorkerState&) = delete;

		void Stop()
		{
			shouldStop.store(true, std::memory_order_relaxed);
			q.Stop();
		}

		bool IsStopping() const
		{
			return shouldStop.load(std::memory_order_relaxed);
		}

		//CQueuesAtm q;
		CQueuesMtx q;
		int64 sum = 0;
		STimestamp popTime{};
		STimestamp sumTime{};
		STimestamp pushTime{};
	private:
		std::atomic_bool shouldStop = false;
	};


	struct alignas(128) SState
	{
		static const size_t alignment = 128;

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
		uint64 s = 0;
		SWork work;
		STimestamp popTime{};
		STimestamp sumTime{};
		STimestamp pushTime{};

		while (true)
		{
			bool popRes;

			{
				STsRegion reg(popTime);
				popRes = state.q.TryPopFromInQueue(work);
			}

			if (popRes)
			{
				{
					STsRegion reg(sumTime);
					s += sum(work.pBuf, work.size);
				}

				{
					STsRegion reg(pushTime);
					while (!state.q.TryPushToOutQueue(work))
						;
				}
			}
			else
			{
				if (state.IsStopping())
					break;
			}
		}

		state.sum = s;
		state.popTime = popTime;
		state.sumTime = sumTime;
		state.pushTime = pushTime;
	}
}

void Test2_Par(FILE* f, const fpos_t fsizePos)
{
	const size_t bufSize = 512 * 1024;

	using namespace Test2;

	std::vector<std::unique_ptr<char[]>> buffers;
	std::vector<char*> freeBuffers;
	buffers.reserve(maxBuffers);
	freeBuffers.reserve(maxBuffers);
	for (int i = 0; i < maxBuffers; ++i)
	{
		buffers.emplace_back(new char[bufSize]);
		freeBuffers.push_back(buffers.back().get());
	}

	std::vector<std::thread> workers;


	const uint32 hw = std::thread::hardware_concurrency();
	//const uint32 hw = 1;
	const uint32 workerCount = std::max(hw, 2u) - 1; // max(hw-1, 1)
	//const uint32 workerCount = std::max(hw, 1u); // max(hw-1, 1)
	//states.reserve(workerCount);

	//std::vector<Test2::SState> states;
	std::unique_ptr<SState[]> states(new SState[workerCount]);
	for (uint32 i = 0; i < workerCount; ++i)
	{
		workers.emplace_back(std::thread(&WorkerFunc, std::ref(states[i].s)));
	}


	fpos_t off = 0;
	size_t read = 0;

	STimestamp sumTime{};
	STimestamp readTime{};
	STimestamp freeBufferFetchTime{};
	STimestamp workPushTime{};
	STimestamp waitingForResultTime{};

	uint32 round = 0;

	auto startTime = ts::now();
	while (off < fsizePos)
	{
		char* pBuf = nullptr;
		{
			STsRegion tsreg(freeBufferFetchTime);
			if (freeBuffers.empty())
			{
				SWork w;
				for (uint32 r = round; ; r = (r + 1) % workerCount)
				{
					if (states[r].s.q.TryPopFromOutQueue(w))
					{
						pBuf = const_cast<char*>(w.pBuf);
						break;
					}
				}
			}
			else
			{
				pBuf = freeBuffers.back();
				freeBuffers.pop_back();
			}
		}

		{
			STsRegion tsreg(readTime);
			read = fread(pBuf, 1, bufSize, f);
		}
		off += read;

		{
			STsRegion reg(workPushTime);
			SWork work{ pBuf, read };
			while (true)
			{
				const uint32 r = round;
				round = (round + 1) % workerCount;
				if (states[r].s.q.TryPushToInQueue(work))
				{
					break;
				}
			}
		}
	}

	uint64 sum = 0;
	{
		STsRegion reg(waitingForResultTime);
		for (SState* s = states.get(); s != states.get() + workerCount; ++s)
			s->s.Stop();

		for (auto& t : workers)
			t.join();

		sum = 0;
		for (SState* s = states.get(); s != states.get() + workerCount; ++s)
			sum += s->s.sum;
	}

	auto endTime = ts::now();

	const int64 expectedSum = 1226802104644;
	if (sum != expectedSum)
		__debugbreak();

	STimestamp workersPopTime{};
	STimestamp workersPushTime{};
	for (SState* s = states.get(); s != states.get() + workerCount; ++s)
	{
		workersPopTime += s->s.popTime;
		workersPushTime += s->s.pushTime;
		sumTime += s->s.sumTime;
	}


	std::cout << __FUNCTION__ << std::endl;
	std::cout << "Total took  " << ms((endTime - startTime).dur()) << " - MB/s " << MBsec(fsizePos, (endTime - startTime)) << std::endl;
	std::cout << "Read took   " << ms(readTime.dur()) << " - MB/s " << MBsec(fsizePos, readTime) << std::endl;
	std::cout << "Sum took    " << ms(sumTime.dur()) << " - MB/s " << MBsec(fsizePos, sumTime) << std::endl;
	std::cout << "Sum/wr took " << ms(sumTime.dur() / workerCount) << " - MB/s " << MBsec(fsizePos, sumTime.dur() / workerCount) << std::endl;
	std::cout << "freeBufferFetchTime   " << ms(freeBufferFetchTime.dur()) << std::endl;
	std::cout << "workPushTime          " << ms(workPushTime.dur()) << std::endl;
	std::cout << "waitingForResultTime  " << ms(waitingForResultTime.dur()) << std::endl;
	std::cout << "workersPopTime        " << ms(workersPopTime.dur()) << std::endl;
	std::cout << "workersPushTime       " << ms(workersPushTime.dur()) << std::endl;
	std::cout << "Sum is " << sum << std::endl;
	std::cout << "workerCount " << workerCount << std::endl;
	std::cout << std::endl;
}
