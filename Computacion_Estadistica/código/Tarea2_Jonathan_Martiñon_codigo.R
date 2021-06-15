# Martiñon Luna Jonathan José
# Tarea 2
# Adaptacion al codigo visto en clase

rm(list = ls())
dev.off()

#Copula Clayton con parametro theta > 1

C = function(u,v, theta = 1) (theta+1)* (u*v)^(-(theta+1))* (u^(-theta) + v^(-theta) - 1)^(-((2*theta+1)/(theta)))

#Haremos M-H para las distribuciones marginales y Beta(0.5,0.5) y Gamma(20,4)

f = function(x1,x2,theta=1) C(pbeta(x1,0.5,0.5),pgamma(x2,20,4),theta)*dbeta(x1,0.5,0.5)*dgamma(x2,20,4)

#Necesitamos esta biblioteca para extraer funciones de la Normal Multivariada
library(mvtnorm)

M_H = function(Nsim=10000,theta0=1,seed=1234){
  
  set.seed(seed)
  
  g1 = function(x1) exp(x1)
  g1_inv = function(x1) log(x1)
  
  g2 = function(x2) exp(x2)
  g2_inv = function(x2) log(x2) 
  
  X = matrix(NA, Nsim+1,2)
  X[1,] = c(rbeta(1,0.5,0.5),rgamma(1,20,4))
  counter = c()
  
  for(i in 1:Nsim){
    U <- runif(1)
    Z_n <- rmvnorm(1,c(g1_inv(X[i,1]),g2_inv(X[i,2])))
    Y_n <- c(g1(Z_n[1]),g2(Z_n[2]))
    
    ratio = min(0,
                log(f(Y_n[1],Y_n[2],theta =theta0)*Y_n[1]*(Y_n[2]))-log(f(X[i,1],X[i,2],theta0)*X[i,1]*(X[i,2])))
    if(log(U) <= ratio){
      X[i+1,] = Y_n
      counter <- c(counter,1)
    } else {
      X[i+1,] = X[i,]
      counter <- c(counter,0)
    }
  }
  
  X = X[-1,]
  
  return(list(X=X,counter=counter))
}

main = function(Nsim=10000,theta0=1,seed=1234){
  samples = M_H(Nsim=Nsim,theta0=theta0,seed=seed)
  X = samples$X
  
  #final_truncExp = curve(dgamma(x,20,4),0,10,add = FALSE)
  #final_logNormal = curve(dbeta(x,0.5,0.5),0,1,add = FALSE)
  
  par(mfrow=c(2,2)) 
  
  plot(X[,1],type="l",ylab="X1", main = paste(c("Theta:",theta0), collapse=" "))
  hist(X[,1],freq = F,main="Distribución para X_1 Beta (0.5,0.5)",
       xlab = "X1")
  curve(dbeta(x,0.5,0.5),0,1,add=TRUE)
  
  
  plot(X[,2],type="l",ylab="X2", main = paste(c("Theta:",theta0), collapse=" "))
  hist(X[,2],freq=F,breaks=40,main="Distribución para X_2 Gamma (20,4)",
       xlab = "X2")
  curve(dgamma(x,20,4),0,10,add=TRUE)
  
  
}

# Mandamos ejecutar la función para cada uno de las thetas
main(theta0 = 0.1)
main(theta0 = 1)
main(theta0 = 10)

