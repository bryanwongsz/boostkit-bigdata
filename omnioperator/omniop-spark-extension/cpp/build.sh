 #!/bin/bash
set -eu
CURRENT_DIR=$ (cd "$(dirname "$BASH SOURCE")"; pwd)
echo SCURRENT DIR
cd S(CURRENT DIR)
曰if ［ -d build ］； then
rm -r build
fi
mkdir build
cd build
# options
曰if ［＄＃ ！＝0］；then
options=""
if [$1= 'debug' ]; then
echo"--Enable Debug"
options="Soptions -DCMARE BUILD TYPE=Debug -DDEBUG RUNTIME=ON"
elif [ $1 - 'trace' ]; then
echo" Enable Trace"
options="Soptions -DCMARE BUILD_TYPE=Debug -DTRACE_RUNTIME=ON"
elif [ $1 -'release' ];then
echo "-- Enable Release"
options="Soptions -DCMAKE_BUILD TYPE=Release"
elif [ $1 = 'test' ];then
echo "-Enable Test"
options="Soptions -DCMAKE_BUILD_TYPE=Test -DBUILD_CPP_TESTS=TRUE" else
echo "-- Enable Release"
options="Soptions -DCMAKE BUILD TYPE=Release"
fi
cmake..
Soptions
else
echo"
Enable Release"
cmake ..-DCMAKE BUILD_TYPE-Release
fi
make
set teu