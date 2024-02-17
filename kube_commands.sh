az login

az account set --subscription 4693832c-ac40-4623-80b9-79a0345fcfce

az acr login --name imperialswemlsspring2024
az aks get-credentials --resource-group imperial-swemls-spring-2024 --name imperial-swemls-spring-2024 --overwrite-existing

kubelogin convert-kubeconfig -l azurecli

kubectl --namespace=peace get pods
docker build --platform=linux/amd64 -t imperialswemlsspring2024.azurecr.io/coursework5-peace .
# docker build -t imperialswemlsspring2024.azurecr.io/coursework4-peace .
docker push imperialswemlsspring2024.azurecr.io/coursework5-peace

kubectl apply -f kubernetes.yaml

kubectl --namespace=peace get deployments

kubectl logs --namespace=peace -l app=aki-detection

# kubectl --namespace=peace delete deployment aki-detection