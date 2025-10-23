# Cargar paquetes
library(dplyr)           # Para manipulación de datos
library(tidyr)           # Para organizar datos en formato tidy
library(lubridate)       # Para manejo de fechas y horas
library(networkDynamic)  # Para redes que tienen temporalidad ?
library(tsna)            # Para análisis de redes temporales ?

# Función para cargar datos

cargar_datos <- function(n_muestra = 1000000) {
  datos <- read.csv("yjmob100k-dataset1.csv.gz", 
                    nrows = n_muestra,  #Limité usando una muestra
                    stringsAsFactors = FALSE) %>%
    rename(
      persona_id = uid,
      dia = d, # d = día (0-74, representan 75 días)
      timeslot = t, # t = intervalo de 30 minutos (0-47, 0=00:00-00:30, 47=23:30-00:00)
      coord_x = x, # x = coordenada X en grilla 200x200 (1-200)
      coord_y = y # y = coordenada Y en grilla 200x200 (1-200)
    ) %>%
    mutate(celda_id = paste(coord_x, coord_y, sep = "_"))
  
  print(paste("Muestra cargada:", n_muestra, "registros")) #Agregué esto para saber el proceso
  return(datos)
}


# Construir red de movilidad
construir_red_movilidad <- function(datos) {
  # Identificar movimientos entre celdas
  movimientos <- datos %>%
    arrange(persona_id, dia, timeslot) %>%
    group_by(persona_id) %>%
    mutate(
      celda_origen = lag(celda_id),
      timeslot_origen = lag(timeslot)
    ) %>%
    ungroup() %>%
    filter(
      !is.na(celda_origen),
      celda_id != celda_origen,
      (timeslot - timeslot_origen) <= 6
    )
  
  #Agregué estas líneas momentáneamente para ir liberando memoria
  rm(datos) 
  gc()
  
  # Preparar estructura temporal
  edge_spells <- movimientos %>%
    mutate(intervalo = floor(timeslot / 6)) %>%
    group_by(celda_origen, celda_id, intervalo) %>%
    summarise(volumen = n_distinct(persona_id), .groups = 'drop') %>%
    mutate(
      onset = intervalo * 3,
      terminus = (intervalo + 1) * 3,
      weight = volumen
    ) %>%
    separate(celda_origen, into = c("origen_x", "origen_y"), sep = "_", convert = TRUE) %>%
    separate(celda_id, into = c("destino_x", "destino_y"), sep = "_", convert = TRUE) %>%
    mutate(
      tail = paste(origen_x, origen_y, sep = "_"),
      head = paste(destino_x, destino_y, sep = "_")
    ) %>%
    select(onset, terminus, tail, head, weight)
  
  # Crear red dinámica
  nodos_unicos <- unique(c(edge_spells$tail, edge_spells$head))
  
  red_dinamica <- networkDynamic(
    edge.spells = edge_spells,
    vertex.spells = data.frame(onset = 0, terminus = 24, vertex.id = nodos_unicos),
    create.TEAs = TRUE,
    edge.TEA.names = "weight"
  )
  
  # Agregar coordenadas a nodos
  info_nodos <- data.frame(node_id = nodos_unicos) %>%
    separate(node_id, into = c("x", "y"), sep = "_", convert = TRUE, remove = FALSE)
  
  for(i in 1:length(nodos_unicos)) {
    set.vertex.attribute(red_dinamica, "coord_x", info_nodos$x[i], i)
    set.vertex.attribute(red_dinamica, "coord_y", info_nodos$y[i], i)
  }
  
  return(list(red = red_dinamica, edge_spells = edge_spells))
}


# Analizar corredores principales
analizar_corredores <- function(red_dinamica) {
  momentos <- list(manana = 8, tarde = 14, noche = 20)
  resultados <- list()
  
  for (nombre in names(momentos)) {
    hora <- momentos[[nombre]]
    red_momento <- network.collapse(red_dinamica, at = hora)
    
    if (network.size(red_momento) > 0) {
      # Métricas de nodos
      metricas <- data.frame(
        momento = nombre,
        node_id = network.vertex.names(red_momento),
        grado_entrada = degree(red_momento, cmode = "indegree"),
        betweenness = betweenness(red_momento, gmode = "digraph")
      )
      
      # Flujos de aristas
      edge_list <- as.edgelist(red_momento)
      edge_weights <- get.edge.value(red_momento, "weight")
      
      if (length(edge_weights) > 0) {
        flujos <- data.frame(
          momento = nombre,
          origen = network.vertex.names(red_momento)[edge_list[, 1]],
          destino = network.vertex.names(red_momento)[edge_list[, 2]],
          flujo = edge_weights
        ) %>% arrange(desc(flujo))
        
        resultados[[nombre]] <- list(metricas = metricas, flujos = flujos)
      }
    }
  }
  
  return(resultados)
}

# Identificar corredores principales
identificar_principales <- function(corredores_temporales) {
  # Consolidar flujos
  todos_flujos <- do.call(rbind, lapply(corredores_temporales, function(x) x$flujos))
  
  top_corredores <- todos_flujos %>%
    group_by(origen, destino) %>%
    summarise(
      flujo_promedio = mean(flujo),
      momentos_activos = n(),
      .groups = 'drop'
    ) %>%
    arrange(desc(flujo_promedio)) %>%
    head(10)
  
  # Nodos importantes
  todas_metricas <- do.call(rbind, lapply(corredores_temporales, function(x) x$metricas))
  
  nodos_importantes <- todas_metricas %>%
    group_by(node_id) %>%
    summarise(
      betweenness_promedio = mean(betweenness),
      .groups = 'drop'
    ) %>%
    arrange(desc(betweenness_promedio)) %>%
    head(10)
  
  return(list(top_corredores = top_corredores, nodos_importantes = nodos_importantes))
}

# Guardar resultados
guardar_resultados <- function(red_dinamica, corredores_principales) {
  saveRDS(red_dinamica, "red_movilidad_yjmob100k.rds")
  saveRDS(corredores_principales, "corredores_principales.rds")
  write.csv(corredores_principales$top_corredores, "top_corredores.csv", row.names = FALSE)
  write.csv(corredores_principales$nodos_importantes, "nodos_importantes.csv", row.names = FALSE)
}

# Ejecutar análisis completo
datos <- cargar_datos()
resultados_red <- construir_red_movilidad(datos)
red_dinamica <- resultados_red$red
corredores_temporales <- analizar_corredores(red_dinamica)
corredores_principales <- identificar_principales(corredores_temporales)
guardar_resultados(red_dinamica, corredores_principales)

