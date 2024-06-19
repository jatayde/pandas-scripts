import pandas as pd

# Load the CSV file
file_path = 'METIAB Trayecto_5feb24.xlsx - Trayectos.csv'
data = pd.read_csv(file_path)

# Clean the dataframe by renaming columns and converting date columns to datetime format
data.columns = data.columns.str.strip()
data['Fecha Llegada \n(Mes/Dia/Año)'] = pd.to_datetime(data['Fecha Llegada \n(Mes/Dia/Año)'], errors='coerce', format='%m/%d/%Y')
data['Fecha Partida (Mes/Dia/Año)'] = pd.to_datetime(data['Fecha Partida (Mes/Dia/Año)'], errors='coerce', format='%m/%d/%Y')

# Define a function to expand rows
def expand_rows(row):
    expanded = []
    if row['Porcion Viaje'] == 1:
        if pd.notna(row['Fecha Partida (Mes/Dia/Año)']) and row['Fecha Partida (Mes/Dia/Año)'].year >= 2020:
            new_row = row.copy()
            new_row['Fecha'] = row['Fecha Partida (Mes/Dia/Año)']
            expanded.append(new_row)
    else:
        start_date = row['Fecha Llegada \n(Mes/Dia/Año)']
        end_date = row['Fecha Partida (Mes/Dia/Año)']
        
        if pd.isna(start_date) and pd.isna(end_date):
            expanded.append(row)
        else:
            if pd.isna(start_date):
                start_date = end_date
            if pd.isna(end_date):
                end_date = start_date
            
            for single_date in pd.date_range(start_date, end_date):
                new_row = row.copy()
                new_row['Fecha'] = single_date
                expanded.append(new_row)
    
    return expanded

# Process the data in smaller chunks
chunk_size = 100
chunks = [data.iloc[i:i + chunk_size] for i in range(0, data.shape[0], chunk_size)]

expanded_chunks = []

for chunk in chunks:
    expanded_chunk = pd.concat([pd.DataFrame(expand_rows(row)) for idx, row in chunk.iterrows()])
    expanded_chunks.append(expanded_chunk)

# Concatenate all expanded chunks
expanded_data = pd.concat(expanded_chunks)

# Drop the original date columns and keep only the new 'Fecha' column
expanded_data = expanded_data.drop(columns=['Fecha Llegada \n(Mes/Dia/Año)', 'Fecha Partida (Mes/Dia/Año)'])

# Select only the required columns in the desired order and rename them accordingly
expanded_data = expanded_data[['Folio', 'Fecha', 'Latitude', 'Longitude', 'Elevation (meters)', 
                               'Pueblo, Municipio Partida', 'País', 'Tiempo Ubicacion', 'Metodo de Transporte (Tiempo en Transito)', 'Comentarios']]

expanded_data.columns = ['ID (Folio)', 'Date', 'Latitude', 'Longitude', 'Elevation (meters)', 'City/Town', 
                         'Country', 'Hours at Location', 'Transit Mode (as Departed from this column\'s location)', 'comments']

# Save the expanded dataframe to a new CSV file
expanded_file_path = 'expanded_trayectos.csv'
expanded_data.to_csv(expanded_file_path, index=False)

print(f"Expanded data saved to {expanded_file_path}")
