all: gpfunc_for_cdc.o gpfunc_for_cdc.so

gpfunc_for_cdc.o:
	gcc -fPIC -c gpfunc_for_cdc.c -I/home/gpadmin/gpdb7_bin/include/postgresql/server/

gpfunc_for_cdc.so:
	gcc -shared -o gpfunc_for_cdc.so gpfunc_for_cdc.o

clean:
	rm -rf *.o *.so
