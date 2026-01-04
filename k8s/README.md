# Run Kubernetes cluster

## 1. Create a cluster using Minikube
- Prereq: **Minikube** and **kubectl** installed
- Run: 
```
minikube start --memory <memory> --cpus <num-cpus>
```
Configure memory and number of CPUs designated to the cluster. By default (if not configured), 2GB memory and 1 core.

## 2. Commands
- Apply manifest files to run services: `kubectl apply -f <file-path>`
- View list of resources: `kubectl get <resource-type>`

    + List of namespaces: `kubectl get namespaces`

    + List of pods/nodes: change `namespaces` to `pods`/`nodes`

    + List of services in a namespace: `kubectl -n <namespace> get <kind>`

## 3. To run the system
- Ensure cluster is created and running (can use `kubectl get` commands above to check)

- Refer to README.md in subdirectories for detailed setups for each application

    + Install Helm charts: Strimzi,
        ```
        helm repo add strimzi https://strimzi.io/charts/
        helm repo update

        helm install strimzi-release strimzi/strimzi-kafka-operator --namespace=bigdata-pipeline --create-namespace
        ```
    
    + Mount Spark application codes into Minikube to be mounted into Spark pods:
        ```
        nohup minikube mount ./src/spark:/mnt/spark/jobs > minikube-mount.log 2>&1 &
        ```
    
    + Open tunnel to access Web UIs/Dashboards; also required to allow Strimzi to create Kafka cluster (?):
        ```
        nohup minikube tunnel &
        ```

- Shortcut to run the system: Assuming you are in the top-level directory of this repo, run:
```
kubectl apply -k k8s/
```
This applies resources defined in the `kustomization.yaml` file found in this `k8s` directory. Kustomize config files are found in each subdirectory to support using this command.

- Create namespace for system (already included in `kustomization.yaml` and automatically created when installing Strimzi):
```
kubectl apply -f namespace.yaml
```

- Access Web UIs of apps in the system:

- To stop the cluster:

    + End background Minikube commands:
        ```
        pkill -f "minikube tunnel"
        pkill -f "minikube mount"
        ```
    
    + Stop Minikube (or delete entirely):
        ```
        minikube stop       # to stop
        minikube delete     # to delete
        ```