- `OVERLAPPED` IO doesn't mean it's unblocking. When file happens to be in OS cached memory, it will serve it from cache and copy pages right on the same thread invoking `ReadFile`, which may take long time (100-s of ms) and context switches. It still notifies IOCP about result.
See https://github.com/MicrosoftDocs/SupportArticles-docs/blob/main/support/windows/win32/asynchronous-disk-io-synchronous.md, which describes this behavior as well. 
File size doesn't seem to be a limitation as long as there is enough free RAM. OS is happy to serve 10 GB file at 13 GB/s, instead of 3 GB/s from disk.
- Unbuffered IO with `FILE_FLAG_NO_BUFFERING` avoids cache, but at the same time - load speed is lower (well, more real).
