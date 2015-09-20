mvn clean package

mkdir StreamPlantform;
cd StreamPlantform;
cp -r bin;
cp -r conf;

mkdir lib;
cp ../core/target/streaming-core-1.0-SNAPSHOT.jar lib;
cp ../event/mc-signal/target/streaming-event-mc-1.0-SNAPSHOT.jar lib;

