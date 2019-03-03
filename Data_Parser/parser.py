# http://www.andymboyle.com/2011/11/02/quick-csv-to-json-parser-in-python/

import csv
import json

# Open the CSV
f = open(r'C:\Users\tallu\Documents\Programming\CS504\Project\data\registered_voters_xtabs_edited.csv')

# Change each fieldname to the appropriate field name
reader = csv.DictReader(f, fieldnames = ("SD",
                                         "C_18_24",
                                         "C_25_34",
                                         "C_35_49",
                                         "C_50_64",
                                         "C_65+",
                                         "C_Unknown",
                                         "AA_18_24",
                                         "AA_25_34",
                                         "AA_35_49",
                                         "AA_50_64",
                                         "AA_65+",
                                         "AA_Unknown",
                                         "H_18_24",
                                         "H_25_34",
                                         "H_35_49",
                                         "H_50_64",
                                         "H_65+",
                                         "H_Unknown",
                                         "A_18_24",
                                         "A_25_34",
                                         "A_35_49",
                                         "A_50_64",
                                         "A_65+",
                                         "A_Unknown",
                                         "NA_18_24",
                                         "NA_25_34",
                                         "NA_35_49",
                                         "NA_50_64",
                                         "NA_65+",
                                         "NA_Unknown",
                                         "Uncoded_18_24",
                                         "Uncoded_25_34",
                                         "Uncoded_35_49",
                                         "Uncoded_50_64",
                                         "Uncoded_65+",
                                         "Uncoded_Unknown",
                                         "Unknown_18_24",
                                         "Unknown_25_34",
                                         "Unknown_35_49",
                                         "Unknown_50_64",
                                         "Unknown_65+",
                                         "Unknown_Unknown",
                                         "M_18_24",
                                         "M_25_34",
                                         "M_35_49",
                                         "M_50_64",
                                         "M_65+",
                                         "M_Unknown",
                                         "F_18_24",
                                         "F_25_34",
                                         "F_35_49",
                                         "F_50_64",
                                         "F_65+",
                                         "F_Unknown",
                                         "Unknown_18_24",
                                         "Unknown_25_34",
                                         "Unknown_35_49",
                                         "Unknown_50_64",
                                         "Unknown_65+",
                                         "Unknown_Unknown",
                                         "Total"
                                         ))

# Parse the CSV into JSON
out = json.dumps([ row for row in reader ])
print ("JSON parsed!")

# Save the JSON
f = open(r'C:\Users\tallu\Documents\Programming\CS504\Project\data\parsed.json', 'w')
f.write(out)
print ("JSON saved!")