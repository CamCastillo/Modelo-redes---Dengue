# Cargar paquetes
library(dplyr)           # Para manipulación de datos
library(tidyr)           # Para organizar datos en formato tidy
library(networkDynamic)  # Para redes con temporalidad
library(tsna)            # Para análisis de redes temporales

# Función para cargar kis datos individuales y transformarlos en flujos agregados
cargar_y_transformar_flujos <- function(n_muestra = 1000000) {
  datos <- read.csv("yjmob100k-dataset1.csv.gz", 
                    nrows = n_muestra,
                    stringsAsFactors = FALSE) %>%
    rename(
      persona_id = uid,
      dia = d,
      timeslot = t,
      coord_x = x,
      coord_y = y
    ) %>%
    mutate(celda_id = paste(coord_x, coord_y, sep = "_"))
  
  print(paste("Datos individuales cargados:", n_muestra, "registros"))
  
  # Transformar en flujos agregados (como creo que es el formato Arica)
  flujos_agregados <- datos %>%
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
    ) %>%
    mutate(
      periodo_temporal = case_when(
        timeslot %in% 0:11 ~ "madrugada",    # 00:00-05:59
        timeslot %in% 12:23 ~ "mañana",      # 06:00-11:59
        timeslot %in% 24:35 ~ "tarde",       # 12:00-17:59
        timeslot %in% 36:47 ~ "noche"        # 18:00-23:59
      )
    ) %>%
    group_by(celda_origen, celda_id, periodo_temporal) %>%
    summarise(
      volumen = n_distinct(persona_id),  # Número de personas únicas
      .groups = 'drop'
    ) %>%
    rename(
      origen = celda_origen,
      destino = celda_id
    )
  
  print(paste("Flujos agregados generados:", nrow(flujos_agregados), "conexiones"))
  
  rm(datos)
  gc()
  
  return(flujos_agregados)
}

# Construir red desde FLUJOS AGREGADOS
construir_red_desde_flujos <- function(datos_flujos) {
  print("Construyendo red desde flujos agregados...") #Para tener el apoyo visual
  
  mapeo_temporal <- list(
    madrugada = c(0, 6),    # 00:00-06:00
    mañana = c(6, 12),      # 06:00-12:00
    tarde = c(12, 18),      # 12:00-18:00
    noche = c(18, 24)       # 18:00-24:00
  )
  
  # Preparar edge spells desde flujos agregados
  edge_spells <- datos_flujos %>%
    mutate(
      onset = sapply(periodo_temporal, function(p) mapeo_temporal[[p]][1]),
      terminus = sapply(periodo_temporal, function(p) mapeo_temporal[[p]][2]),
      weight = volumen
    ) %>%
    select(onset, terminus, tail = origen, head = destino, weight)
  
  nodos_unicos <- unique(c(edge_spells$tail, edge_spells$head))
  
  # Crear red dinámica
  red_dinamica <- networkDynamic(
    edge.spells = edge_spells,
    vertex.spells = data.frame(onset = 0, terminus = 24, vertex.id = nodos_unicos),
    create.TEAs = TRUE,
    edge.TEA.names = "weight"
  )
  
  info_nodos <- data.frame(node_id = nodos_unicos) %>%
    separate(node_id, into = c("x", "y"), sep = "_", convert = TRUE, remove = FALSE)
  
  for(i in 1:length(nodos_unicos)) {
    set.vertex.attribute(red_dinamica, "coord_x", info_nodos$x[i], i)
    set.vertex.attribute(red_dinamica, "coord_y", info_nodos$y[i], i)
  }
  
  print(paste("Red de flujos construida:", network.size(red_dinamica), "zonas,",
              nrow(edge_spells), "flujos temporales"))
  
  return(list(red = red_dinamica, edge_spells = edge_spells))
}


# Analizar corredores principales (IGUAL)
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
    head(10) #revisar
  
  return(list(top_corredores = top_corredores, nodos_importantes = nodos_importantes))
}

guardar_resultados <- function(red_dinamica, corredores_principales) {
  saveRDS(red_dinamica, "red_flujos_bangkok.rds")
  saveRDS(corredores_principales, "corredores_flujos_bangkok.rds")
  write.csv(corredores_principales$top_corredores, "top_corredores_flujos.csv", row.names = FALSE)
  write.csv(corredores_principales$nodos_importantes, "nodos_importantes_flujos.csv", row.names = FALSE)
  
  print("Resultados guardados:")
  print("- red_flujos_bangkok.rds")
  print("- corredores_flujos_bangkok.rds")
  print("- top_corredores_flujos.csv")
  print("- nodos_importantes_flujos.csv")
}

# VERIFICACIÓN DE COMPATIBILIDAD CON EPIMODEL (porque lo voy a usar para el objetivo 3 de simular)
verificar_epimodel <- function(red_dinamica) {
  checks <- c(
    "networkDynamic" = inherits(red_dinamica, "networkDynamic"),
    "tiene_pesos" = !is.null(get.edge.attribute(red_dinamica, "weight")),
    "nodos_positivos" = network.size(red_dinamica) > 0,
    "puede_colapsar" = network.size(network.collapse(red_dinamica, at = 12)) > 0
  )
  
  print("Verificación EpiModel:")
  for(i in 1:length(checks)) { #Más intuitivo con emoticones
    status <- ifelse(checks[i], "✅", "❌")
    print(paste(status, names(checks)[i]))
  }
  
  return(all(checks))
}

# EJECUCIÓN CON FLUJOS AGREGADOS
print("EJECUTANDO CON FLUJOS AGREGADOS (Formato final)")

# 1. Cargar y transformar datos individuales en flujos
flujos_agregados <- cargar_y_transformar_flujos(n_muestra = 1000000)

# 2. Construir red desde flujos agregados
resultados_red <- construir_red_desde_flujos(flujos_agregados)
red_dinamica <- resultados_red$red

# 3. Análisis (este debería ser idéntico)
corredores_temporales <- analizar_corredores(red_dinamica)
corredores_principales <- identificar_principales(corredores_temporales)

# 4. Guardar resultados
guardar_resultados(red_dinamica, corredores_principales)

# 5. Verificar compatibilidad
epimodel_compatible <- verificar_epimodel(red_dinamica)

print("")
if(epimodel_compatible) {
  print("¡Código listo para avanzar!")
} else {
  print("Revisar compatibilidad antes de avanzar")
}