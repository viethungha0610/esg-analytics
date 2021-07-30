import pandas as pd

# Specific orgs to remove from the graphs
ORGS_TO_REMOVE = ["bloomberg", "reuters", "new york times", "united states", "nasdaq", "facebook", "twitter", "european union", "cnn", "white house"]
IMPORT_DIR = "C:/Users/ZH834BT/.Neo4jDesktop/relate-data/dbmss/dbms-2eaf39b9-8ef9-4ae3-9b71-1447ea06ddfb/import"

def main():
    print("Cleaning Gdelt graph data ...")

    # Filtering out the ORGS_TO_REMOVE from data
    print("Reading nodes file ...")
    gdelt_nodes_df = pd.read_csv("gdelt_graph_nodes.csv")
    filtered_gdelt_nodes_df = gdelt_nodes_df[~gdelt_nodes_df['organisation'].isin(ORGS_TO_REMOVE)]
    print("Reading edges file ...")
    gdelt_edges_df = pd.read_csv("gdelt_graph_edges.csv")
    print(f"Original shape: {gdelt_edges_df.shape}")
    filtered_gdelt_edges_df = gdelt_edges_df[~(gdelt_edges_df['src'].isin(ORGS_TO_REMOVE))
                                            &~(gdelt_edges_df['dst'].isin(ORGS_TO_REMOVE))]

    # Removing duplicates
    filtered_gdelt_edges_df['src_dst_tup'] = filtered_gdelt_edges_df.apply(
        lambda x: str(sorted([x['src']] + [x['dst']])), axis=1
    )
    filtered_gdelt_edges_df.drop_duplicates(subset='src_dst_tup', inplace=True)
    filtered_gdelt_edges_df.drop(columns=['src_dst_tup'], inplace=True)
    print(f"New shape: {filtered_gdelt_edges_df.shape}")

    # Export to Neo4j DMBS's Import folder
    print("Exporting ...")
    filtered_gdelt_edges_df.to_csv(f"{IMPORT_DIR}/gdelt_graph_edges.csv", index=False)
    filtered_gdelt_nodes_df.to_csv(f"{IMPORT_DIR}/gdelt_graph_nodes.csv", index=False)
    print("Done!")
    
if __name__ == '__main__':
    main()