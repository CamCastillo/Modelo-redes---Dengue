# OBJETIVO 2: INTEGRACIÓN DE DATOS DEL VECTOR 

# Librerías para los datos del vector

library(data.table)
library(networkDynamic)
library(sna)


#Cargar red de movilidad y verificar estructura
cat("Cargando red de movilidad existente...\n")
red_movilidad <- readRDS("red_movilidad_completa.rds")
zonas_red <- get.vertex.attribute(red_movilidad, "zona_id")
n_zonas <- length(zonas_red)
cat("Red cargada:", n_zonas, "zonas\n")
cat("Atributos actuales:", list.vertex.attributes(red_movilidad), "\n")

#Generar datos del vector
cat("Generando datos del vector sintéticos...\n")

#Distribución realista (10% alto riesgo, 20% medio, 70% bajo)
datos_vectoriales <- data.table(
  zona_id = zonas_red,
  
  #Distribución heterogénea típica
  densidad_vectorial = {
    riesgo <- numeric(n_zonas)
    alto <- sample(1:n_zonas, size = max(1, n_zonas * 0.10))
    medio <- sample(setdiff(1:n_zonas, alto), size = max(1, n_zonas * 0.20)) 
    bajo <- setdiff(1:n_zonas, c(alto, medio))
    
    riesgo[alto] <- runif(length(alto), 0.7, 0.95)
    riesgo[medio] <- runif(length(medio), 0.3, 0.7)
    riesgo[bajo] <- runif(length(bajo), 0.1, 0.3)
    riesgo
  },
  
  # Distribución urbana 
  contexto_ambiental = sample(
    c("intra_domiciliario", "extra_domiciliario", "espacio_publico", "zona_verde"),#Ver zonas
    size = n_zonas,
    replace = TRUE,
    prob = c(0.45, 0.30, 0.15, 0.10)  # Basado en estudios urbanos
  ),
  
  # Factor estacional
  factor_estacional = runif(n_zonas, 0.6, 1.4)
)

cat("Datos vectoriales generados para", n_zonas, "zonas\n")

#Mapeo e integración en nodos
cat("Integrando atributos vectoriales en nodos...\n")

# Verificar correspondencia
zonas_coincidentes <- intersect(zonas_red, datos_vectoriales$zona_id)
cat("Zonas con datos vectoriales:", length(zonas_coincidentes), "/", n_zonas, "\n")

#Asignación vectorizada
mapeo_integracion <- data.table(
  indice_vertice = 1:n_zonas,
  zona_id = zonas_red
)[datos_vectoriales, on = "zona_id", nomatch = 0]

cat("Registros para integrar:", nrow(mapeo_integracion), "\n")

set.vertex.attribute(red_movilidad, "densidad_vectorial", 
                     mapeo_integracion$densidad_vectorial, 
                     mapeo_integracion$indice_vertice)

set.vertex.attribute(red_movilidad, "contexto_ambiental", 
                     mapeo_integracion$contexto_ambiental, 
                     mapeo_integracion$indice_vertice)

set.vertex.attribute(red_movilidad, "factor_estacional", 
                     mapeo_integracion$factor_estacional, 
                     mapeo_integracion$indice_vertice)

# Verificación
cat("Atributos después de integración:", list.vertex.attributes(red_movilidad), "\n")
cat("Integración completada\n")

#Identificación de nodos críticos
cat("Identificando nodos críticos...\n")

# Métricas de centralidad
red_medio_dia <- network.collapse(red_movilidad, at = 12) #Hora de máxima movilidad

# Calcular métricas combinadas
metricas_nodos <- data.table(
  zona_id = zonas_red,
  
  # Centralidad de movilidad
  grado_entrada = degree(red_medio_dia, cmode = "indegree"),
  betweenness = betweenness(red_medio_dia, gmode = "digraph"),
  
  # Riesgo vectorial
  densidad_vectorial = get.vertex.attribute(red_movilidad, "densidad_vectorial"),
  contexto_ambiental = get.vertex.attribute(red_movilidad, "contexto_ambiental"),
  factor_estacional = get.vertex.attribute(red_movilidad, "factor_estacional")
)

# Riesgo integrado (fórmula epidemiológica). Revisar
metricas_nodos[, `:=`(
  movilidad_norm = if (max(grado_entrada) > 0) grado_entrada / max(grado_entrada) else 0,
  vectorial_norm = if (max(densidad_vectorial) > 0) densidad_vectorial / max(densidad_vectorial) else 0
)]

# Combinar con pesos (50% movilidad, 50% vectorial - revisar)
metricas_nodos[, riesgo_integrado := (movilidad_norm * 0.5) + (vectorial_norm * 0.5)]

# Clasificar nodos críticos
metricas_nodos[, categoria_riesgo := cut(riesgo_integrado,
                                         breaks = c(0, 0.33, 0.66, 1),
                                         labels = c("Bajo", "Medio", "Alto"),
                                         include.lowest = TRUE)]

cat("Análisis de nodos críticos:\n")

# Top 10 nodos más críticos
nodos_criticos <- metricas_nodos[order(-riesgo_integrado)][1:10]
cat("\nTOP 10 NODOS CRÍTICOS:\n")
print(nodos_criticos[, .(zona_id, riesgo_integrado, categoria_riesgo, grado_entrada, densidad_vectorial)])

# Distribución por contexto ambiental
distribucion_contexto <- metricas_nodos[, .(
  n_zonas = .N,
  riesgo_promedio = mean(riesgo_integrado),
  porcentaje_alto_riesgo = sum(categoria_riesgo == "Alto") / .N * 100
), by = contexto_ambiental]

cat("\nDISTRIBUCIÓN POR CONTEXTO AMBIENTAL:\n")
print(distribucion_contexto)

# Resumen estadístico
cat("\nRESUMEN ESTADÍSTICO - RIESGO INTEGRADO:\n")
cat("Mediana:", round(median(metricas_nodos$riesgo_integrado), 3), "\n")
cat("Rango: [", round(min(metricas_nodos$riesgo_integrado), 3), ", ", 
    round(max(metricas_nodos$riesgo_integrado), 3), "]\n", sep = "")
cat("Zonas alto riesgo:", metricas_nodos[categoria_riesgo == "Alto", .N], 
    "(", round(metricas_nodos[categoria_riesgo == "Alto", .N] / n_zonas * 100, 1), "%)\n")

#Exportar resultados
cat("Exportando resultados...\n")
saveRDS(red_movilidad, "red_movilidad_con_vectorial.rds")
fwrite(metricas_nodos, "metricas_nodos_criticos.csv")
fwrite(datos_vectoriales, "datos_vectoriales_sinteticos.csv")

cat("\nARCHIVOS GENERADOS:\n")
cat("- red_movilidad_con_vectorial.rds (red integrada para EpiModel)\n")
cat("- metricas_nodos_criticos.csv (análisis de nodos críticos)\n")
cat("- datos_vectoriales_sinteticos.csv (datos vectoriales utilizados)\n")
