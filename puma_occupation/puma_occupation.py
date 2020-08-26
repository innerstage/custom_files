import requests
import json
import pandas as pd
import csv


# Let's get a list of PUMAs geoids first:
pumas_df = pd.read_csv("pumas.csv")
pumas_name_map = {k:v for (k,v) in zip(pumas_df["geoid"], pumas_df["name"])}

# Define a query function
def make_query(puma):
    query = "https://keyite-api.datausa.io/cubes/pums_1/aggregate?drilldown%5B%5D=%5BPUMS+Occupation%5D.%5BDetailed+Occupation%5D&cut%5B%5D=%5BGeography%5D.%5BGeography%5D.%5BPUMA%5D.%26%5B{}%5D&measures%5B%5D=Total+Population&measures%5B%5D=Average+Wage&nonempty=true&distinct=false&parents=false&debug=true&sparse=true".format(puma)
    r = requests.get(query)
    json_r = json.loads(r.text)

    occupation_id_list = [json_r["axes"][1]["members"][i]["key"] for i in range(len(json_r["axes"][1]["members"]))]
    occupation_name_list = [json_r["axes"][1]["members"][i]["name"] for i in range(len(json_r["axes"][1]["members"]))]
    population_values = [json_r["values"][i][0] for i in range(len(json_r["values"]))]
    wage_values = [json_r["values"][i][1] for i in range(len(json_r["values"]))]

    df = pd.DataFrame(
            {
                "puma_id": puma, "puma_name": pumas_name_map[puma], "occupation_id": occupation_id_list,
                "occupation_name": occupation_name_list, "total_population": population_values, "average_wage": wage_values
            }
        )

    df.to_csv("final_file.csv", index=False, mode="a", quoting=csv.QUOTE_NONNUMERIC)

    return 0


# Query for each PUMA
N = len(list(pumas_name_map.keys()))
a = 0

for puma in list(pumas_name_map.keys()):
    a += 1
    print("{:,}/{:,} ({:.2f}%) Processing {}".format(a, N, a/N*100, puma))
    try:
        make_query(puma)
    except:
        with open("problematic_pumas.log","a") as file:
            file.write("{}\n".format(puma))
        continue
    
