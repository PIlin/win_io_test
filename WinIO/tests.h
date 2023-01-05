#pragma once

#include "common.h"

void Test1_Seq(FILE* f, const fpos_t fsizePos);
void Test2_Par(FILE* f, const fpos_t fsizePos);
void Test3_CompIOWorkers(const char* szFilename, const fpos_t fsizePos);
void Test4_DStorage(const char* szFilename, const fpos_t fsizePos);
