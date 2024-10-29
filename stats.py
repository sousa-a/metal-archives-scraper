import pandas as pd
import networkx as nx
import matplotlib.pyplot as plt
import os
import re

# Load the metal_bands.csv file
bands_df = pd.read_csv('sample_data/metal_bands.csv')

# Define color scheme for nodes
color_map = {
    'genre': 'red',  # Color for genre nodes
    'band': 'blue',  # Color for band nodes
}

# Create a base NetworkX graph with all genres and bands
G = nx.Graph()
for index, row in bands_df.iterrows():
    band_name = row['Name']
    genre = row['Genre']
    
    # Add genre node if not present
    if genre not in G:
        G.add_node(genre, type='genre')
    
    # Add band node
    G.add_node(band_name, type='band')
    
    # Connect band to genre
    G.add_edge(band_name, genre)

# Create a directory to store the images
output_dir = 'statistics'
os.makedirs(output_dir, exist_ok=True)

def sanitize_filename(name):
    # Replace problematic characters
    name = re.sub(r'[<>:"/\\|?*]', '_', name)  # Replace invalid filename characters
    name = re.sub(r'\s+', '_', name)           # Replace spaces with underscores
    name = re.sub(r'\(.*?\)', '', name)        # Remove parentheses and their contents
    name = re.sub(r';+', '_', name)            # Replace semicolons with underscores
    name = name.strip('_')                     # Remove leading/trailing underscores
    return name

# Generate and save a subgraph for each genre
for genre in bands_df['Genre'].unique():
    # Create a subgraph with the genre and its bands
    genre_subgraph = G.subgraph([genre] + 
                                [node for node, data in G.nodes(data=True) 
                                 if data.get('type') == 'band' and genre in G[node]])

    # Define color mapping for the nodes in the subgraph
    node_colors = [
        color_map['genre'] if G.nodes[node].get('type') == 'genre' else color_map['band']
        for node in genre_subgraph.nodes
    ]

    # Sanitize the genre name for the filename
    sanitized_genre = sanitize_filename(genre)

    # Draw and save the subgraph
    plt.figure(figsize=(8, 6))
    pos = nx.spring_layout(genre_subgraph, seed=42)  # Position layout
    nx.draw_networkx_nodes(genre_subgraph, pos, node_color=node_colors, node_size=100)
    nx.draw_networkx_edges(genre_subgraph, pos, alpha=0.5)
    nx.draw_networkx_labels(genre_subgraph, pos, font_size=8)

    # Save the figure to the 'statistics' folder with sanitized filename
    plt.title(f"Genre: {genre} - Bands Connection")
    plt.axis('off')
    plt.savefig(os.path.join(output_dir, f"{sanitized_genre}_bands_network.png"))
    plt.close()  # Close the figure to save memory
