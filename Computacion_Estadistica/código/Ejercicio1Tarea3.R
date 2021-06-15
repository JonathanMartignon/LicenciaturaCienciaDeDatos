####################################
# Ejercicio 4 de la tarea examen 2
# Martiñón Luna Jonathan José
####################################


# Ajustamos semilla, limpiamos el entorno y ajustamos el directorio
rm(list = ls())
graphics.off()
set.seed(2209)
setwd("~/Documentos/Estadistica")

library(ggplot2)

N <- 800
n <- sum(rbinom(N, 1, 0.6))

x_1 <- rnorm(N - n, 0, 0.5)
x_2 <- rnorm(n, 2, 0.3)
#qplot(c(x_1, x_2))

crearLogLike <- function(x){
  logLike <- function(theta){
    pi <- exp(theta[1]) / (1 + exp(theta[1]))
    mu_1 <- theta[2]
    mu_2 <- theta[3]
    sigma_1 <- exp(theta[4])
    sigma_2 <- exp(theta[5])
    sum(log(pi*dnorm(x, mu_1, sd=sigma_1)+(1-pi)*dnorm(x,mu_2,sd=sigma_2)))
  }
  logLike
}
func_1 <- crearLogLike(c(x_1,x_2))
system.time(salida <- optim(c(0.5,0,0,1,1), func_1, control=list(fnscale=-1)))

