#pragma once

#define _CRT_SECURE_NO_WARNINGS

#include <cstdint>
#include <chrono>

#include "wininclude.h"

#define USE_PIX
#include <pix3.h>

#define PROF_FUNC() PIXScopedEvent(PIX_COLOR_INDEX(0), __FUNCTION__)
#define PROF_REGION(NAME) PIXScopedEvent(PIX_COLOR_INDEX(1), NAME)

using uint64 = uint64_t;
using uint32 = uint32_t;
using uint8 = uint8_t;
using int64 = int64_t;

struct STimestamp
{
	static STimestamp now()
	{
		LARGE_INTEGER time;
		QueryPerformanceCounter(&time);
		return STimestamp{ time.QuadPart };
	}

	static int64 GetFrequency()
	{
		LARGE_INTEGER freq{};
		QueryPerformanceFrequency(&freq);
		return freq.QuadPart;
	}

	std::chrono::high_resolution_clock::duration dur() const
	{
		const int64 freq = GetFrequency();
		using period = std::chrono::high_resolution_clock::period;
		using duration = std::chrono::high_resolution_clock::duration;
		// copied from std::chrono::high_resolution_clock::now()
		const long long _Whole = (time / freq) * period::den;
		const long long _Part = (time % freq) * period::den / freq;
		return duration(_Whole + _Part);
	}

	std::chrono::high_resolution_clock::time_point ts() const
	{
		using time_point = std::chrono::high_resolution_clock::time_point;
		return time_point(dur());
	}

	STimestamp operator+(const STimestamp other) const { return STimestamp{ time + other.time }; }
	STimestamp operator-(const STimestamp other) const { return STimestamp{ time - other.time }; }

	STimestamp& operator+=(const STimestamp other) { time += other.time; return *this; }

	int64 time;
};
using ts = STimestamp;

struct STsRegion
{
	STsRegion(STimestamp& t)
		: accum(t)
		, start(ts::now())
	{}

	~STsRegion()
	{
		accum += (ts::now() - start);
	}

	STimestamp& accum;
	STimestamp start{};
};

template <typename D> constexpr auto ms(const D& d) { return std::chrono::duration_cast<std::chrono::duration<double, std::milli>>(d).count(); }
template <typename D> constexpr auto sec(const D& d) { return std::chrono::duration_cast<std::chrono::duration<double, std::ratio<1, 1>>>(d).count(); }
template <typename D> auto MBsec(size_t size, const D& d) { return (size / sec(d)) / 1024 / 1024; }
inline auto MBsec(size_t size, const STimestamp& d) { return (size / sec(d.dur())) / 1024 / 1024; }

inline uint64 sum(const char* pBuf, size_t size)
{
	uint64 s = 0;
	for (const char* p = pBuf; p < (pBuf + size); ++p)
	{
		s += (uint8)*p;
	}
	return s;
}

void SetThreadName(const wchar_t* format, ...);
