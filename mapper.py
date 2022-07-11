from multiprocessing import Process, Manager, Lock
from os import cpu_count
from time import perf_counter

import pandas as pd
import numpy as np


def cleaner(villages: list) -> list:
    count = 0
    clean_villages = []
    for v in villages:
        if ',' in v:
            l = []
            for x in v.split(','):
                if x.strip() not in l:
                    l.append(x.strip())
            v = ', '.join(l).strip() if len(l) > 1 else l[0]
            count += 1
        clean_villages.append(v)
    print(count, 'raw representations cleaned!')

    return clean_villages


def mapper(villages: list, found_tees: list=[], lock=None):
    found_tokens: list[str] = []
    need_mapping: list[str] = []
    found_village_index: list[int] = []
    unmapped_village_index: list[int] = []
    got_or_added = False
    
    for i, v in enumerate(villages):
        for token, mapping in tokens.items():
            if v.strip() == token.strip():
                found_tokens.append(mapping)
                found_village_index.append(i)
                break
        else:
            got_or_added = False
            got_token = []
            got_token_str = ""
            vs = []

            # sepearating each word
            vs = v.replace(',', "").split()
            vs = [split_word.strip() for split_word in vs if split_word.strip()]
            v_str = v

            # iterating over each word to see if they have a token
            for x in vs:
                next_char = ""
                for t, m in tokens.items():
                    next_char = ""
                    if x.strip() == t.strip():
                        got_or_added = True
                        try:
                            slice_index = v_str.index(x) + len(x)
                            next_char = v_str[slice_index]
                            v_str = v_str[slice_index:]
                        except IndexError:
                            next_char = ""
                        got_token_str = f'{got_token_str}{m}{next_char}'
                        match next_char:
                            case " ":
                                pass
                            case ",":
                                got_token_str = got_token_str + " "
                        got_token.append(m)
                        break
                    
                else:
                    if x not in need_mapping:
                        got_or_added = True
                        need_mapping.append(x)
                        unmapped_village_index.append(i)

            if got_or_added:
                found_tokens.append(got_token_str.strip())
                found_village_index.append(i)

        if not got_or_added and v not in need_mapping:
            need_mapping.append(v)
            unmapped_village_index.append(i)

        if i % 1000 == 0:
            print('done with:', i)
    
    if lock is not None:
        lock.acquire()
        found_tees.append((found_village_index, found_tokens, unmapped_village_index, need_mapping))
        lock.release()

    print(f'Found: {len(found_village_index)}, Unmapped: {len(unmapped_village_index)}')
    return found_village_index, found_tokens, unmapped_village_index, need_mapping


def write_to_ward_file(mapped_df: pd.DataFrame, unmapped_df: pd.DataFrame) -> None:
    mapped_df.to_csv('mapped_ward_check.csv', index=False)
    unmapped_df.to_csv('unmapped_ward_check.csv', index=False)
    return

    
def write_to_village_file(mapped_df: pd.DataFrame, unmapped_df: pd.DataFrame) -> None:
    mapped_df.to_csv('mapped_village_check.csv', index=False)
    unmapped_df.to_csv('unmapped_village_check.csv', index=False)


def manipulate_data(raw_villages):
    # manipulating data
    clean_villages = cleaner(raw_villages)
    found_village_index, found_tokens, unmapped_village_index, need_mapping = mapper(clean_villages)
    print(f'{len(found_tokens)=}\n{len(found_village_index)=}\n{len(unmapped_village_index)=}\n{len(need_mapping)=}')
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

def multi_processing_manipulate_data(raw_villages):
    # manipulating data
    clean_villages = cleaner(raw_villages)
    processes = []
    num_processes = cpu_count()
    print(f'starting {num_processes} processes')
    with Manager() as ma:
        l = Lock()
        processes = []
        slices = np.array_split(clean_villages, num_processes)
        found_tees = ma.list()
        for i in range(num_processes):
            p = Process(target=mapper, args=(slices[i], found_tees, l, ))
            p.start()
            processes.append(p)
        for p in processes:
            p.join()
            
        found_village_index = []
        found_tokens = []
        unmapped_village_index = []
        need_mapping = []
        
        # unpacking data from the multiprocessing queue
        for i in range(num_processes):
            found_village_index.extend(found_tees[i][0])
            found_tokens.extend(found_tees[i][1])
            unmapped_village_index.extend(found_tees[i][2])
            need_mapping.extend(found_tees[i][3])

    print(len(found_village_index), 'found')
    
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


def main():
    # Reading files
    # df = pd.read_csv('up_unmapped_wards.csv')
    df = pd.read_csv('up_unmapped_villages.csv')
    tokens_df = pd.read_csv('wards.csv')
    v_tokens_df = pd.read_csv('new_villages.csv')

    # getting data
    og_reps = tokens_df['og_representation'].tolist()
    mappings = tokens_df['mapping'].tolist()
    og_reps.extend(v_tokens_df['og_representation'].tolist())
    mappings = tokens_df['mapping'].tolist()
    mappings.extend(v_tokens_df['mapping'].tolist())
    global tokens
    tokens = {str(og): str(map) for og, map in zip(og_reps, mappings)}
    # tokens = dict(zip(og_reps, mappings, strict=True))
    raw_villages = df['village_raw'].tolist()
    # raw_villages = df['ward_raw'].tolist()
    # mapped_df, unmapped_df = manipulate_data(raw_villages)
    mapped_df, unmapped_df = multi_processing_manipulate_data(raw_villages)
    # write_to_ward_file(mapped_df, unmapped_df)
    # write_to_village_file(mapped_df, unmapped_df)

    
if __name__ == "__main__":
    start = perf_counter()
    try:
        main()
    finally:
        end = perf_counter()
        print(end-start)
