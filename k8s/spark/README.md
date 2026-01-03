- Mount directory: (runs in background; change local path as desired)
```
nohup minikube mount ./src/spark/jobs:/mnt/spark/jobs > minikube-mount.log 2>&1 & 
```
Later to stop the mount: `pkill -f "minikube mount"`

- Apply as usual: `kubectl apply -f k8s/spark/`

- Open tunnel to check web UI: `minikube tunnel`

    + `localhost:8082` for Spark Master UI

    + `localhost:4040` for Spark Driver UI (available when a Spark job is submitted and running)

- Todo: Submit job to test + introduce Airflow