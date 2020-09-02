DIR="./build/"
pip uninstall -y Doctopus 1>nul
cd ../..
if [ -d "$DIR" ]; then
    rm -rf "$DIR"
    echo 'clean build/'
fi
python setup.py install 
