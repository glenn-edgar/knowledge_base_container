cp -r ../building_blocks  building_blocks
docker build . -t nanodatacenter/python_base_image_lite
rm -r building_blocks

