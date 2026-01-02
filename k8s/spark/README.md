- Mount directory: (runs in background; change local path as desired)
```
nohup minikube mount ./src/spark/jobs:/mnt/spark/jobs > minikube-mount.log 2>&1 & 
```
Later to stop the mount: `pkill -f "minikube mount"`

- Apply as usual: `kubectl apply -f k8s/spark/`

- Open tunnel to check web UI: `minikube tunnel` -> `localhost:8082`

- Todo: Submit job to test + introduce Airflow