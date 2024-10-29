import pandas as pd
import networkx as nx
import matplotlib.pyplot as plt
import os
import re

def load_data(file_path):
    """Load the metal_bands.csv file."""
    return pd.read_csv(file_path)

def create_graph(bands_df):
    """Create a base NetworkX graph with all genres and bands."""
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
    return G

def sanitize_filename(name):
    """Sanitize the filename by replacing problematic characters."""
    name = re.sub(r'[<>:"/\\|?*]', '_', name)  # Replace invalid filename characters
    name = re.sub(r'\s+', '_', name)           # Replace spaces with underscores
    name = re.sub(r'\(.*?\)', '', name)        # Remove parentheses and their contents
    name = re.sub(r';+', '_', name)            # Replace semicolons with underscores
    name = name.strip('_')                     # Remove leading/trailing underscores
    return name

def save_genre_subgraph(G, genre, output_dir, color_map):
    """Generate and save a subgraph for a specific genre with improved layout."""
    genre_subgraph = G.subgraph(
        [genre] + [node for node, data in G.nodes(data=True) 
                   if data.get('type') == 'band' and genre in G[node]]
    )

    # Define edge colors based on node types
    edge_colors = [
        color_map['genre'] if G.nodes[node].get('type') == 'genre' else color_map['band']
        for node in genre_subgraph.nodes
    ]

    # Different sizes for genre and band nodes
    node_sizes = [
        300 if G.nodes[node].get('type') == 'genre' else 600
        for node in genre_subgraph.nodes
    ]

    sanitized_genre = sanitize_filename(genre)

    plt.figure(figsize=(10, 8))
    pos = nx.spring_layout(genre_subgraph, seed=42, k=0.5)  # Adjust spring layout

    # Draw nodes with border color and no fill
    nx.draw_networkx_nodes(
        genre_subgraph,
        pos,
        node_size=node_sizes,
        node_color='none',         # Set fill to transparent
        edgecolors=edge_colors,     # Border colors
        linewidths=2,              # Set border thickness
        alpha=1                    # Fully opaque borders
    )

    # Draw edges with different styles
    nx.draw_networkx_edges(
        genre_subgraph, pos, alpha=0.5, edge_color='gray', style='dotted'
    )
    
    # Adjust label positions to be below the nodes
    label_pos = {node: (x, y - 0.1) for node, (x, y) in pos.items()}  # Shift downwards

    # Draw labels at adjusted positions
    nx.draw_networkx_labels(
        genre_subgraph,
        label_pos,  # Use the adjusted positions for labels
        font_size=10,
        font_color='black',
        font_weight='bold',
        bbox=dict(facecolor='none', edgecolor='none'),  # No background for labels
    )

    plt.title(f"Genre: {genre} - Bands Connection", fontsize=14)
    plt.axis('off')
    
    plt.savefig(os.path.join(output_dir, f"{sanitized_genre}_bands_network.png"))
    plt.close()  # Close the figure to save memory


def main():
    # Load the data
    bands_df = load_data('sample_data/metal_bands.csv')

    # Define color scheme for nodes
    color_map = {
        'genre': 'red',  # Color for genre nodes
        'band': 'blue',  # Color for band nodes
    }

    # Create the graph
    G = create_graph(bands_df)

    # Create a directory to store the images
    output_dir = 'statistics/genre'
    os.makedirs(output_dir, exist_ok=True)

    # Generate and save subgraphs for each genre
    for genre in bands_df['Genre'].unique():
        save_genre_subgraph(G, genre, output_dir, color_map)

if __name__ == "__main__":
    main()
