extendJars="/home/leo/MySource/StreamPlatform/ShanDongFeature/target/xx.jar"

rm OCDP_Stream.tar.gz

mvn clean package;

mkdir OCDP_Stream;
cd OCDP_Stream;
cp -r ../bin .;
cp -r ../conf .;

mkdir lib;
cp ../core/target/core-1.0-SNAPSHOT.jar lib;
cp $extendJars lib;

mkdir logs;

cd ..;
tar zcvf OCDP_Stream.tar.gz OCDP_Stream;
rm -r OCDP_Stream;

