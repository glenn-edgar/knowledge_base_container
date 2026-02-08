rm test_driver

gcc -o test_driver test_driver.c -I/usr/include/postgresql -I../include -L../libs -lkb_data_structures_static -lm -lcjson  -lpq -luuid
