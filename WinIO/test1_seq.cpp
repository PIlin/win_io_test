#include "tests.h"

#include <iostream>


void Test1_Seq(FILE* f, const fpos_t fsizePos)
{
	const size_t bufSize = 64 * 1024;
	std::unique_ptr<char[]> pBuf(new char[bufSize]);

	fpos_t off = 0;
	size_t read = 0;

	uint64 s = 0;

	auto startTime = ts::now();
	STimestamp sumTime{};
	STimestamp readTime{};


	while (off < fsizePos)
	{
		{
			auto ss = ts::now();
			read = fread(pBuf.get(), 1, bufSize, f);
			auto se = ts::now();

			readTime += se - ss;
		}
		off += read;

		{
			auto ss = ts::now();
			s += sum(pBuf.get(), read);
			auto se = ts::now();

			sumTime += se - ss;
		}
	}

	auto endTime = ts::now();

	const int64 expectedSum = 1226802104644;
	if (s != expectedSum)
		__debugbreak();

	std::cout << __FUNCTION__ << std::endl;
	std::cout << "Total took " << ms((endTime - startTime).dur()) << " - MB/s " << MBsec(fsizePos, (endTime - startTime)) << std::endl;
	std::cout << "Read took  " << ms(readTime.dur()) << " - MB/s " << MBsec(fsizePos, readTime) << std::endl;
	std::cout << "Sum took   " << ms(sumTime.dur()) << " - MB/s " << MBsec(fsizePos, sumTime) << std::endl;
	std::cout << "Sum is " << s << std::endl;
	std::cout << std::endl;
}
