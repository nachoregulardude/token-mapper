import pandas as pd
from os import listdir

def cleaner(villages: list[str]) -> list[str]: 
    count = 0
    clean_villages = []
    for v in villages:
        if ',' in v:
            l = []
            for x in v.split(','):
                if x.strip().endswith('1'):
                    x = x[:-1].strip()
                    
                if x.strip() not in l:
                    l.append(x.strip())
                    
            v = ', '.join(l).strip() if len(l) > 1 else l[0]
            count += 1
        clean_villages.append(v)
    print(count, 'raw representations cleaned!')

    return clean_villages


def mapper(villages: list[str]) -> tuple[list]:
    found_tokens: list[str] = []
    need_mapping: list[str] = []
    found_village_index: list[int] = []
    unmapped_village_index: list[int] = []
    got = False
    added = False
    
    for i, v in enumerate(villages):
        try:
            found_tokens.append(TOKENS[v])
            found_village_index.append(i)
            got = True
        
        except KeyError:
            got = False
            added = False
            got_token_str = ""
            vs = v.replace(',', "").split()
            vs = [split_word.strip() for split_word in vs if split_word.strip()]
            v_str = v
            # iterating over each word to see if they have a token
            for x in vs:
                added = False
                next_char = ""
                try:
                    mapping = TOKENS[x.strip()]
                    got= True
                    try:
                        slice_index = v_str.index(x) + len(x)
                        next_char = v_str[slice_index]
                        v_str = v_str[slice_index:]
                    except IndexError:
                        next_char = ""
                        
                    got_token_str = f'{got_token_str}{mapping}{next_char}'
                    match next_char:
                        case ",":
                            got_token_str = f'{got_token_str} '
                except KeyError:
                    if x not in need_mapping:
                        added = True
                        need_mapping.append(x)
                        unmapped_village_index.append(i)
            
            if got:
                found_tokens.append(got_token_str.strip())
                found_village_index.append(i)
        
        if not got and not added and v not in need_mapping and not all(x in need_mapping for x in vs):
            need_mapping.append(v)
            unmapped_village_index.append(i)

        if i % 1000 == 0:
            print('done with:', i)
    print(f'Found: {len(found_village_index)}, Unmapped: {len(unmapped_village_index)}')
    
    return found_village_index, found_tokens, unmapped_village_index, need_mapping


def write_to_ward_file(mapped_df: pd.DataFrame, unmapped_df: pd.DataFrame) -> None:
    mapped_df.to_csv('mapped_ward.csv', index=False)
    unmapped_df.to_csv('unmapped_ward.csv', index=False)
    print('files created')
    return None

    
def write_to_village_file(mapped_df: pd.DataFrame, unmapped_df: pd.DataFrame) -> None:
    mapped_df.to_csv('mapped_village.csv', index=False)
    unmapped_df.to_csv('unmapped_village.csv', index=False)
    print('file created')
    return None


def manipulate_data(raw_villages: list[str]) -> tuple[pd.DataFrame]:
    # manipulating data
    clean_villages = cleaner(raw_villages)
    found_village_index, found_tokens, unmapped_village_index, need_mapping = mapper(clean_villages)
    print('---------------------')
    print(f'{len(found_tokens)=}\n{len(need_mapping)=}')
    print('---------------------')
    raw_clean_mapped_village_list = [(raw_villages[i], clean_villages[i]) for i in found_village_index]
    raw_clean_unmapped_village_list = [(raw_villages[i], clean_villages[i]) for i in unmapped_village_index]
    raw_mapped_villages, clean_mapped_villages = zip(*raw_clean_mapped_village_list)
    raw_unmapped_villages, clean_unmapped_villages = zip(*raw_clean_unmapped_village_list)
    
    # creating dataframes
    mapped_df = pd.DataFrame({
        'raw_villages': raw_mapped_villages,
        'clean_villages': clean_mapped_villages,
        'tokens': found_tokens
    })
    unmapped_df = pd.DataFrame({
        'raw_village': raw_unmapped_villages,
        'clean_village': clean_unmapped_villages,
        'need_mapping_for': need_mapping
    })

    return mapped_df, unmapped_df


def main() -> None:
    # Reading files
    csvs = [file for file in listdir() if file.endswith('.csv')]
    _ = [print(f'{i}. {file}') for i, file in enumerate(csvs, 1)]
    choice_unmapped = int(input("Enter the index of the unmapped file: ")) - 1
    choice_tokens = int(input("Enter the index of the tokens file: ")) - 1
    df = pd.read_csv(csvs[choice_unmapped])
    tokens_df = pd.read_csv(csvs[choice_tokens])

    if ('og_representation' and 'mapping' not in list(tokens_df.columns)) or ('village_raw' not in list(df.columns)):
        raise ValueError("check column names or the file chosen")
        
    # loading in data from files
    og_reps = tokens_df['og_representation'].tolist()
    mappings = tokens_df['mapping'].tolist()
    raw_villages = df['village_raw'].tolist()
    global TOKENS
    TOKENS = {str(og).strip(): str(map).strip() for og, map in zip(og_reps, mappings)}
    # raw_villages = df['ward_raw'].tolist()
    mapped_df, unmapped_df = manipulate_data(raw_villages)
    # write_to_ward_file(mapped_df, unmapped_df)
    # write_to_village_file(mapped_df, unmapped_df)
    return None

    
if __name__ == "__main__":
    main()
