To build the LSB tree, run:
lsb.exe -b [Page Size] -d [Dim of Feature] -ds [File_Path] -f [Output_File] -l [Number of Trees] -n [Number of Feature] -t [Max Coordinate]


e.g. 
lsb.exe -b 16384 -d 10 -ds D:\Ph.D\Expreiments\lsb_float\dataset\Movie_10.txt -f D:\Ph.D\Expreiments\lsb_float\Movie-forest-10-16KB -l 1 -n 1350 -t 5

e.g.
lsb.exe -b 524288 -d 10 -ds D:\Ph.D\Expreiments\lsb_float\dataset\Yelp_10.txt -f D:\Ph.D\Expreiments\lsb_float\Yelp-forest-10-0.5MB -l 1 -n 24103 -t 7


To generate Z value of queries, run:
lsb.exe -f [Tree_File_path] -k [K neighbor] -l [Number of Trees] -o [Output_Zvaule_File] -r [Accurate_File] -q [Quey_File]