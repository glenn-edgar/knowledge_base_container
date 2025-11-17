
cp  -r ../building_blocks/    building_blocks
docker build . -t nanodatacenter/python_base_image
rm  -r building_blocks

