Build custom image for Kafka Connect containing the desired plugin.
```
docker build -f docker/kafka-connect.Dockerfile demo/kafka-minio/ -t kafka-connect-s3:latest
```
Note that paths are specified locally:
- `demo/kafka-minio/` is where the S3 Sink Connector plugin resides in the folder `connect-plugins`
- Here, the above path consists a `.dockerignore` file that ignores the `connect-plugins` folder. Comment out/Remove the folder name in `.dockerignore` to build the image

Make modifications accordingly to either the command or the Dockerfile to rebuild the image if needed

Push to Docker Hub for later pull. (image already built with name and tag specified in `kafka-connect.yaml`)