title py3reinstall-Doctopus
py -3 -m pip uninstall -y Doctopus 1>nul
cd ../..
if exist build rd /s /q build
echo 'clean build/'
py -3 setup.py install 1>nul
echo 'py -3 reinstall good'