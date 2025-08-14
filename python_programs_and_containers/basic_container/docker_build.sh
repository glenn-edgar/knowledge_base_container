
cp  -r ../common_libraries/    common_libraries
docker build . -t nanodatacenter/python_base_image
rm  -r common_libraries

