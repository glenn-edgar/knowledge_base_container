Usage: luajit codegen.lua [options] <schema.lua>

Options:
  -o, --output <dir>      Output directory (default: current directory)
  -p, --prefix <name>     Override output file prefix (default: schema name)
  -n, --no-bin            Skip binary file generation (.bin)
  -b, --bin-only          Generate only binary file (.bin)
  -d, --debug             Include debug string tables in _hashes.h
  -c, --c-only            Generate only C headers (no .bin)
  -j, --json              Generate JSON sidecar file
  -v, --verbose           Verbose output
  -q, --quiet             Suppress output except errors
  -h, --help              Show this help message
  --no-hashes             Skip hash reference header generation
  --validate-only         Validate schema without generating files
  --dump-tree             Print tree structure to stdout
  --dump-hashes           Print all hashes to stdout
  --endian <le|be>        Binary endianness (default: le)
Examples

