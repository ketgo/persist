# Persist

ACID Data Store

## Build

The CMake build system is required to build the project. Run the following command to trigger the build:
```bash
    $ mkdir build && cd build
    $ cmake ..
    $ make
```

The above command will create a DEBUG build. For running code quality, coverage, and sanitization checks, provide the following options to the above `cmake`:
```bash
    $ cmake .. -DCPPCHECK=ON -DCODE_COVERAGE=ON -DUSE_SANITIZER=Thread
```
CMake will now create targets for coverage with the naming convention `ccov-persist_test`. Moreover, the cppCheck tool will print code quality notifications when the source code file is getting compiled. The sanitizers are tools, as part of compiler, that performs checks during a program’s runtime and returns issues. Different compiler flags are set depending on the value set for `USE_SANITIZER`. The possible values are:

- `Address`
- `Memory`
- `MemoryWithOrigins`
- `Undefined`
- `Thread`
- `Address;Undefined`
- `Undefined;Address`
- `Leak`

## License

The source code is under MIT license.

## Contributions

Pull requests and bug reports are always welcomed! :)
