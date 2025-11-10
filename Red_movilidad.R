# OBJETIVO 1: CONSTRUCCIÓN DE RED DE MOVILIDAD

# Librerías para la red de movilidad
library(data.table)
library(networkDynamic)
library(tsna)


# Lectura  del dataset 
datos <- fread("yjmob100k-dataset2.csv.gz")
setnames(datos, c("uid", "d", "t", "x", "y"), 
         c("persona_id", "dia", "timeslot", "coord_x", "coord_y"))

# Crear identificadores de zona
datos[, zona_id := paste(coord_x, coord_y, sep = "_")]

# Calcular movimientos entre zonas de manera directa
cat("Calculando movimientos entre zonas...\n")
setorder(datos, persona_id, dia, timeslot)

movimientos <- datos[
  , .(zona_actual = zona_id, timeslot_actual = timeslot),
  by = .(persona_id, dia)
][
  , `:=`(zona_anterior = shift(zona_actual),
         timeslot_anterior = shift(timeslot_actual)),
  by = .(persona_id, dia)
]

# Filtrar y clasificar movimientos válidos en un solo paso
flujos_agregados <- movimientos[
  !is.na(zona_anterior) & 
    zona_actual != zona_anterior &
    (timeslot_actual - timeslot_anterior) <= 6
][
  , .(volumen = .N),
  by = .(origen = zona_anterior, destino = zona_actual, dia)
]


cat("Flujos agregados listos:", nrow(flujos_agregados), "conexiones\n")

# Construcción de la red

cat("Construyendo red de movilidad...\n")

edge_spells <- flujos_agregados[
  , .(onset = 0, terminus = 24, tail = origen, head = destino, weight = volumen)  # ← Todo el día
]

# Identificar zonas únicas
zonas_unicas <- unique(c(edge_spells$tail, edge_spells$head))
mapeo_zonas <- data.table(
  zona_id = zonas_unicas,
  id_numerico = 1:length(zonas_unicas)
)

# Convertir tail y head a IDs numéricos
edge_spells_numeric <- copy(edge_spells)
edge_spells_numeric <- edge_spells_numeric[
  mapeo_zonas, 
  on = c("tail" = "zona_id"),
  nomatch = 0
]
edge_spells_numeric[, tail_num := id_numerico]
edge_spells_numeric <- edge_spells_numeric[
  mapeo_zonas, 
  on = c("head" = "zona_id"),
  nomatch = 0
]
edge_spells_numeric[, head_num := id_numerico]

edge_spells_numeric <- edge_spells_numeric[
  , .(onset, terminus, tail = tail_num, head = head_num, weight)
]


# Construir red dinámica
red_movilidad <- networkDynamic(
  edge.spells = edge_spells_numeric,
  vertex.spells = data.frame(
    onset = 0, 
    terminus = 24, 
    vertex.id = 1:length(zonas_unicas)
  ),
  create.TEAs = TRUE,
  edge.TEA.names = "weight"
)


# Agregar identificadores de zona como atributo
network::set.vertex.attribute(red_movilidad, "zona_id", zonas_unicas)

cat("Red construida - Nodos:", network.size(red_movilidad), "\n")
cat("Red construida - Flujos temporales:", nrow(edge_spells), "\n")


# Identificación de corredores principales
cat("\nCorredores principales (5 por volumen diario):\n") 
corredores_principales <- edge_spells[
  , .(volumen_total = sum(weight),
      volumen_promedio_diario = mean(weight),
      dias_activos = .N), 
  by = .(origen = tail, destino = head)
][order(-volumen_promedio_diario)][1:5]   

print(corredores_principales)

# Exportación

saveRDS(red_movilidad, "red_movilidad_completa.rds")
fwrite(flujos_agregados, "flujos_agregados.csv")

cat("\nArchivos exportados:\n")
cat("- red_movilidad_completa.rds (red para modelación)\n")
cat("- flujos_agregados.csv (datos brutos de flujos)\n")

