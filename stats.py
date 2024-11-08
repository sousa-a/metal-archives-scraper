import pandas as pd
import networkx as nx
import matplotlib.pyplot as plt
import matplotlib
import os
import re

matplotlib.rcParams['font.family'] = 'DejaVu Sans'


def adjust_label_positions(pos, adjustment_value):
    """Adjust the vertical (y) position of the labels by a fixed amount."""
    adjusted_pos = pos.copy()
    for node in adjusted_pos:
        x, y = adjusted_pos[node]
        adjusted_pos[node] = (x, y + adjustment_value)
    return adjusted_pos

def save_improved_graph(G, output_path):
    """Improved function for saving a graph visualization with adjusted label positions."""
    plt.figure(figsize=(150, 150))
    
    pos = nx.spring_layout(G, seed=42, k=0.5, iterations=100)
    
    pos_labels = adjust_label_positions(pos, adjustment_value=0.5)
    
    node_sizes = [800 + len(list(G.neighbors(node)))*100 for node in G.nodes]
    
    color_map = {'genre': 'red', 'band': 'blue', 'album': 'green'}
    node_colors = [color_map.get(G.nodes[node].get('type', 'other'), 'gray') for node in G.nodes]
    
    nx.draw_networkx_nodes(G, pos, node_size=node_sizes, node_color=node_colors, edgecolors='black', linewidths=1.5)
    nx.draw_networkx_edges(G, pos, alpha=0.3, edge_color='gray')    
    nx.draw_networkx_labels(G, pos_labels, font_size=12, font_color='black')
    
    plt.title("Improved Network Visualization", fontsize=14)
    plt.axis('off')
    plt.savefig(output_path)
    plt.close()

def load_data(band_file_path, album_file_path):
    """Load the metal_bands.csv and all_bands_discography.csv files."""
    print(f"Loading data from {band_file_path} and {album_file_path}...")
    bands_df = pd.read_csv(band_file_path, low_memory=False)
    albums_df = pd.read_csv(album_file_path, low_memory=False)
    
    bands_df['Band ID'] = bands_df['Band ID'].astype(str).str.strip()
    albums_df['Band ID'] = albums_df['Band ID'].astype(str).str.strip()

    print("Data loaded successfully.")
    return bands_df, albums_df

def create_genre_graph(bands_df):
    """Create a NetworkX graph with genres connected to bands."""
    print("Creating genre graph...")
    genre_graph = nx.Graph()
    
    for _, row in bands_df.iterrows():
        band_name = row['Name']
        genre = row['Genre']
        
        if genre not in genre_graph:
            genre_graph.add_node(genre, type='genre')
        
        genre_graph.add_node(band_name, type='band')
        genre_graph.add_edge(band_name, genre)
    
    print(f"Genre graph created with {len(genre_graph.nodes)} nodes and {len(genre_graph.edges)} edges.")
    return genre_graph

def create_album_graph(bands_df, albums_df):
    """Create a NetworkX graph with bands connected to their albums."""
    print("Creating album graph...")
    album_graph = nx.Graph()
    
    album_count = 0
    for _, row in bands_df.iterrows():
        band_name = row['Name']
        band_id = row['Band ID']
        
        album_graph.add_node(band_name, type='band', band_id=band_id)
    
    for _, row in albums_df.iterrows():
        band_id = row['Band ID']
        album_name = row['Album Name']
        album_type = row['Type']
        album_year = row['Year']
        
        band_row = bands_df[bands_df['Band ID'] == band_id]
        if not band_row.empty:
            band_name = band_row.iloc[0]['Name']
            album_graph.add_node(album_name, type='album', album_type=album_type, year=album_year)
            album_graph.add_edge(band_name, album_name)
            album_count += 1
        else:
            print(f"Warning: Band ID {band_id} in album {album_name} not found in the bands data.")
    
    print(f"Album graph created with {len(album_graph.nodes)} nodes and {album_count} edges.")
    return album_graph

def sanitize_filename(name):
    """Sanitize the filename by replacing problematic characters."""
    name = re.sub(r'[<>:"/\\|?*]', '_', name)  
    name = re.sub(r'\s+', '_', name)
    name = re.sub(r'\(.*?\)', '', name)
    name = re.sub(r';+', '_', name)
    name = name.strip('_')
    return name

def save_graph(G, title, output_path, color_map):
    """General function to save a graph to a file."""
    print(f"Saving graph: {title}...")
    plt.figure(figsize=(20, 20))
    pos = nx.spring_layout(G, seed=42, k=0.5)
    
    node_colors = [color_map.get(G.nodes[node].get('type', 'other'), 'gray') for node in G.nodes]
    node_sizes = [800 if G.nodes[node].get('type') == 'band' else 400 for node in G.nodes]
    
    nx.draw_networkx_nodes(G, pos, node_size=node_sizes, edgecolors='black', linewidths=1.5, node_color='none')
    
    nx.draw_networkx_edges(G, pos, alpha=0.3, edge_color='gray')
    nx.draw_networkx_labels(G, pos, font_size=21, font_color='darkblue', verticalalignment='bottom')
    
    plt.title(title, fontsize=14)
    plt.axis('off')
    plt.savefig(output_path)
    plt.close()
    print(f"Graph saved to {output_path}")

def save_genre_subgraph(genre, genre_graph, output_dir):
    """Save a single genre subgraph with its connected bands."""
    print(f"Saving genre subgraph for genre: {genre}...")
    if genre_graph.nodes[genre].get('type') == 'genre':
        genre_subgraph = genre_graph.subgraph([genre] + list(genre_graph.neighbors(genre)))
        sanitized_genre = sanitize_filename(genre)
        output_path = os.path.join(output_dir, f"{sanitized_genre}_genre_network.png")
        save_graph(genre_subgraph, f"Genre: {genre} - Bands Connection", output_path, color_map={'genre': 'red', 'band': 'blue'})

def save_album_subgraph(band, album_graph, output_dir):
    """Save a single band's album subgraph with its connected albums."""
    print(f"Saving album subgraph for band: {band}...")
    band_subgraph = album_graph.subgraph([band] + list(album_graph.neighbors(band)))
    sanitized_band = sanitize_filename(band)
    output_path = os.path.join(output_dir, f"{sanitized_band}_albums_network.png")
    save_graph(band_subgraph, f"Band: {band} - Albums Connection", output_path, color_map={'album': 'green', 'band': 'blue'})

def main():
    bands_file_path = 'metal_bands.csv'
    albums_file_path = 'bands_discos/all_bands_discography.csv'
    
    bands_df, albums_df = load_data(bands_file_path, albums_file_path)

    color_map = {
        'genre': 'red',
        'band': 'blue',
        'album': 'green'
    }

    genre_graph = create_genre_graph(bands_df)
    album_graph = create_album_graph(bands_df, albums_df)

    output_dir_genre = 'statistics/genres'
    output_dir_album = 'statistics/albums'
    os.makedirs(output_dir_genre, exist_ok=True)
    os.makedirs(output_dir_album, exist_ok=True)

    print("Available genres to save:")
    for genre in genre_graph.nodes:
        if genre_graph.nodes[genre].get('type') == 'genre':
            save_genre_subgraph(genre, genre_graph, output_dir_genre)
            print("Subgraph saved.")

    print("\nAvailable bands to save albums for:")
    for band in bands_df['Name'].unique():
        save_album_subgraph(band, album_graph, output_dir_album)
        print("Subgraph saved.")
        
if __name__ == "__main__":
    main()
