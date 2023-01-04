#include "tests.h"

int main()
{
	const char* szFilename = "f:/code/winio/test/datapc64_merged_bnk_textures1.forge";

	FILE* f = fopen(szFilename, "rb");
	if (!f)
		return 1;

	int res = fseek(f, 0, SEEK_END);
	if (res)
		return 2;

	long fsize = ftell(f);
	long long fsize64 = _ftelli64(f);

	fpos_t fsizePos = 0;
	res = fgetpos(f, &fsizePos);

	for (int i = 0; i < 3; ++i)
	{
		res = fseek(f, 0, SEEK_SET);
		Test1_Seq(f, fsizePos);

		res = fseek(f, 0, SEEK_SET);
		Test2_Par(f, fsizePos);

		Test3_CompIOWorkers(szFilename, fsizePos);
	}

	int isEof = feof(f);
	int isErr = ferror(f);


	return 0;
}

