import pandas as pd
from os import listdir
import re

def cleaner(villages: list[str]) -> list[str]: 
    count = 0
    clean_villages = []
    for v in villages:
        if ',' in v:
            l = []
            for x in v.split(','):
                # if x.strip().endswith('1'):
                #     x = x[:-1].strip()
                    
                if x.strip() not in l:
                    l.append(x.strip())
                    
            v = ', '.join(l).strip() if len(l) > 1 else l[0]
            count += 1
        clean_villages.append(v)
    print(count, 'raw representations cleaned!')

    return clean_villages


def add_remove_space_before_and_after_special_chars(
        address: str, flag: str, non_ascii_chars: list = []
):
    """
        Desc flag='add':
            Example input -> output : flat no: 1/2 -> flat no : 1 / 2
            
            
        Desc flag='remove':
            Example input -> output : flat no: 1/2 -> flat no : 1 / 2
    """
    special_chars_in_address = re.findall(
        r"\W", address.encode("ascii", errors="ignore").decode("ascii")
    )
    
    integers = re.findall(
            r"\d", address.encode("ascii", errors="ignore").decode("ascii")
            )
    special_chars_in_address = special_chars_in_address + non_ascii_chars + integers + [':', '“']

    for char in special_chars_in_address:
        char = char.strip() if isinstance(char, str) else char
        match flag:
            case 'add':
                if char:
                    if char == '.':
                        address = address.replace(f"{char}", f"{char} ")
                    else:
                        address = address.replace(f"{char}", f" {char} ")

            case 'remove':
                if char:
                    if char == '.':
                        address = address.replace(f"{char} ", f"{char}")
                    else: 
                        address = address.replace(f" {char} ", f"{char}")
            case other:
                raise LookupError
    return address

# def remove_space_before_and_after_special_chars(
#     address: str, non_ascii_chars: list = []
# ):
#     """
#         Desc:
#             Example input -> output : flat no: 1/2 -> flat no : 1 / 2
#     """
#     special_chars_in_address = re.findall(
#         r"\W", address.encode("ascii", errors="ignore").decode("ascii")
#     )
#     integers = re.findall(
#             r"\d+", address.encode("ascii", errors="ignore").decode("ascii")
#             )
#     special_chars_in_address = special_chars_in_address + non_ascii_chars + integers
#
#     special_chars_in_address = list(dict.fromkeys(special_chars_in_address))
#     
#     for char in special_chars_in_address:
#         char = char.strip() if isinstance(char, str) else char
#         if char is not None:
#             if char=='.':
#                 address = address.replace(f"{char} ", f"{char}")
#             else:
#                 address = address.replace(f" {char} ", f"{char}")
#     
#     return address

def mapper(villages: list[str]) -> tuple[list]:
    found_tokens: list[str] = []
    need_mapping: list[str] = []
    found_village_index: list[int] = []
    unmapped_village_index: list[int] = []
    got = False
    added = False
    count_villages = []
    
    for i, v in enumerate(villages):
        try:
            found_tokens.append(TOKENS[v])
            found_village_index.append(i)
            got = True
        
        except KeyError:
            got = False
            added = False
            got_token_list = []
            vs = add_remove_space_before_and_after_special_chars(v, flag='add').split(' ')
            
            # vs = [split_word.strip() for split_word in vs if split_word.strip()]
            # v_str = v
            # iterating over each word to see if they have a token

            characters_to_ignore = 'abcdefghijklmnopqrstuvwxyz1234567890. '
            
            for x in vs:
                added = False
                # next_char = ""
                if all(i.lower() in characters_to_ignore for i in x):
                    got_token_list.append(x)
                    got = True
                    continue
                try:
                    got_token_list.append(TOKENS[x.strip()])
                    got= True
                    # try:
                    #     slice_index = v_str.index(x) + len(x)
                    #     next_char = v_str[slice_index]
                    #     v_str = v_str[slice_index:]
                    # except IndexError:
                    #     next_char = ""
                    # got_token_str = f'{got_token_str}{mapping}{next_char}'
                    # match next_char:
                    #     case ",":
                    #         got_token_str = f'{got_token_str} '
                    
                except KeyError:
                    if x not in need_mapping:
                        added = True
                        c = counters(x, villages)
                        count_villages.append(c)
                        need_mapping.append(x)
                        unmapped_village_index.append(i)
            
            if got:
                found_str = ' '.join(got_token_list)
                found_str_clean = add_remove_space_before_and_after_special_chars(found_str, flag='remove')
                found_tokens.append(found_str_clean)
                found_village_index.append(i)
        
        if not got and not added and v not in need_mapping and not all(x in need_mapping for x in vs):
            print(f"something's not right with :{v}")
            c = counters(v, villages)
            count_villages.append(c)
            need_mapping.append(v)
            unmapped_village_index.append(i)

        if i % 1000 == 0:
            print('done with:', i)
    print(f'Found: {len(found_village_index)}, Unmapped: {len(unmapped_village_index)}')
    
    return found_village_index, found_tokens, unmapped_village_index, need_mapping, count_villages


def counters(x, villages):
    count = 0
    # for v in villages:
    #     word_list = [x.strip() for x in add_space_before_and_after_special_chars(v).split(' ')]
    #     count += word_list.count(x.strip())

    count = sum(1 if x in v else 0 for v in villages)
    return count
    
def write_to_ward_file(mapped_df: pd.DataFrame, unmapped_df: pd.DataFrame) -> None:
    mapped_df.to_csv('mapped_ward.csv', index=False)
    unmapped_df.to_csv('unmapped_ward.csv', index=False)
    print('files created')
    return None

    
def write_to_village_file(mapped_df: pd.DataFrame, unmapped_df: pd.DataFrame) -> None:
    mapped_df.to_csv('mapped_village.csv', index=False)
    if unmapped_df is not None:
        unmapped_df.to_csv('unmapped_village.csv', index=False)
    else:
        print('no unmapped tokens')
    print('file created')
    return None


def manipulate_data(raw_villages: list[str]) -> tuple[pd.DataFrame]:
    # manipulating data
    clean_villages = cleaner(raw_villages)
    found_village_index, found_tokens, unmapped_village_index, need_mapping, count_villages = mapper(clean_villages)
    print('---------------------')
    print(f'{len(found_tokens)=}\n{len(need_mapping)=}')
    print('---------------------')
    raw_clean_mapped_village_list = [(raw_villages[i], clean_villages[i]) for i in found_village_index]
    raw_clean_unmapped_village_list = [(raw_villages[i], clean_villages[i]) for i in unmapped_village_index]
    raw_mapped_villages, clean_mapped_villages = zip(*raw_clean_mapped_village_list)
    if raw_clean_unmapped_village_list != []:
        raw_unmapped_villages, clean_unmapped_villages = zip(*raw_clean_unmapped_village_list)
    
    # getting count
    # counts = []
    # for i, x in enumerate(need_mapping):
    #     count = 0
    #     for v in raw_villages:
    #         vs = [v.strip() for v in add_space_before_and_after_special_chars(v).split(' ')]
    #         for w in vs:
    #             if w==x:
    #                 count += 1
    #     print(f'{x}: {count}')
    #     print(i)
    #     counts.append(count)
    
    # creating dataframes
    mapped_df = pd.DataFrame({
        'raw_villages': raw_mapped_villages,
        # 'clean_villages': clean_mapped_villages,
        'tokens': found_tokens
    })
    
    unmapped_df = None
    if raw_clean_unmapped_village_list != []:
        unmapped_df = pd.DataFrame({
            'raw_village': raw_unmapped_villages,
            'clean_village': clean_unmapped_villages,
            'need_mapping_for': need_mapping,
            'count_need_mapping': count_villages
            # 'better_count_need_mapping': counts
        })

    return mapped_df, unmapped_df if unmapped_df else None


def main() -> None:

    # Reading files
    csvs = [file for file in listdir() if file.endswith('.csv')]
    _ = [print(f'{i}. {file}') for i, file in enumerate(csvs, 1)]
    choice_unmapped = int(input("Enter the index of the unmapped file: ")) - 1
    choice_tokens = int(input("Enter the index of the tokens file: ")) - 1
    df = pd.read_csv(csvs[choice_unmapped])
    tokens_df = pd.read_csv(csvs[choice_tokens])

    if ('og_representation' and 'mapping' not in list(tokens_df.columns)) or ('og_representation' not in list(df.columns)):
        raise ValueError("check column names or the file chosen")
        
    _ = [print(f'{i}. {column}') for i, column in enumerate(df.columns, 1)]
    choice_column_for_mapping = df.columns[int(input('Enter the index of the column to be mapped: ')) - 1]
    
    # loading in data from files
    og_reps = tokens_df['og_representation'].tolist()
    mappings = tokens_df['mapping'].tolist()
    raw_villages = df[choice_column_for_mapping].tolist()
    global TOKENS
    TOKENS = {str(og).strip(): str(map).strip() for og, map in zip(og_reps, mappings)}
    # raw_villages = df['ward_raw'].tolist()
    mapped_df, unmapped_df = manipulate_data(raw_villages)
    # write_to_ward_file(mapped_df, unmapped_df)
    write_to_village_file(mapped_df, unmapped_df)
    return None

    
if __name__ == "__main__":
    main()
