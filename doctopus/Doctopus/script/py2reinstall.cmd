title py2reinstall-Doctopus
py -2 -m pip uninstall -y Doctopus 1>nul
cd ../..
if exist build rd /s /q build
echo 'clean build/'
py -2 setup.py install 1>nul
echo 'py -2 reinstall good'