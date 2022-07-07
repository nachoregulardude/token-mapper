import pandas as pd


def cleaner(villages):
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

    return count, clean_villages


def mapper(villages: list, tokens: dict):
    found_tokens: list[str] = []
    need_mapping: list[str] = []
    found_village_index: list[int] = []
    unmapped_village_index: list[int] = []
    got = False
    for i, v in enumerate(villages):
        for token, mapping in tokens.items():
            if v.strip() == token.strip():
                # print(mapping, v)
                found_tokens.append(mapping)
                found_village_index.append(i)
                break
        else:

            got = False
            got_token = []
            got_token_str = ""
            vs = []
            # seperating the words
            #vs = v.split(',') if ',' in v else v.split()
            vs = v.replace(',', "").split(" ")
            vs = [split_word.strip() for split_word in vs if split_word.strip()]
            # if_space = any(' ' in x for x in vs)
            # if if_space:
            #     new = []
            #     for s in vs:
            #         words = s.split(' ')
            #         new.extend(words)
            #     vs = new
            # vs = [x.strip() for x in vs]
            # while '' in vs:
            #     vs.remove('')

            # iterating over each word to see if they have a token
            v_str = v
            for x in vs:
                next_char = ""
                for t, m in tokens.items():
                    next_char = ""
                    if x.strip() == t.strip():
                        # print(f'found {x} \nmapped to {m}')
                        len_x = len(x.strip())
                        try:
                            slice_index = v_str.index(x) + len_x
                            next_char = v_str[slice_index]
                            v_str = v_str[slice_index:]
                        except IndexError:
                            next_char = ""
                        got_token_str = got_token_str + m + next_char
                        # print("X:", x)
                        # print("M:", m)
                        if next_char == " ":
                            pass
                        elif next_char == ",":
                            got_token_str = got_token_str + " "
                        got_token.append(m)
                        got = True
                        break
                else:
                    if x not in need_mapping:
                        need_mapping.append(x)
                        unmapped_village_index.append(i)

            if got:
                got_token_str = got_token_str.strip()
                token = ', '.join(got_token) if ',' in v else ' '.join(got_token)
                # debugging lines
                # print("V", v)
                # print("Vs:", vs)
                # print("Token:", token)
                # print("Got token str:", got_token_str)
                # print()
                found_tokens.append(token)
                found_village_index.append(i)

        if not got and v not in need_mapping:
            need_mapping.append(v)
            unmapped_village_index.append(i)

        if i % 1000 == 0:
            print('done with:', i)

    print(f'Found: {len(found_village_index)}, Unmapped: {len(unmapped_village_index)}')
    return found_village_index, found_tokens, unmapped_village_index, need_mapping


# Reading files
df = pd.read_csv('up_unmapped_wards.csv')
# df = pd.read_csv('up_unmapped_villages.csv')
tokens_df = pd.read_csv('wards.csv')
v_tokens_df = pd.read_csv('new_villages.csv')

og_reps = tokens_df['og_representation'].tolist()
mappings = tokens_df['mapping'].tolist()
og_reps.extend(v_tokens_df['og_representation'].tolist())
mappings = tokens_df['mapping'].tolist()
mappings.extend(v_tokens_df['mapping'].tolist())
tokens = dict(zip(og_reps, mappings, strict=True))
# raw_villages = df['village_raw'].tolist()
raw_villages = df['ward_raw'].tolist()

# manipulating data
count, clean_villages = cleaner(raw_villages)
found_village_index, found_tokens, unmapped_village_index, need_mapping = mapper(clean_villages, tokens)
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
# mapped_df.to_csv('mapped_village_check.csv', index=False)
mapped_df.to_csv('mapped_ward_check.csv', index=False)

unmapped_df = pd.DataFrame({
    'raw_village': raw_unmapped_villages,
    'clean_village': clean_unmapped_villages,
    'need_mapping_for': need_mapping
})
# unmapped_df.to_csv('unmapped_village_check.csv', index=False)
unmapped_df.to_csv('unmapped_ward_check.csv', index=False)

# df['cleaned_village'], df['found_village'], df['found_tokens'] = clean_villages
# df['cleaned'] = clean_villages
# df.to_csv('cleaned.csv', index=False)
