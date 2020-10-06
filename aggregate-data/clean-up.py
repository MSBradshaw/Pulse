if __name__ == '__main__':
    print('Cleaning biorxiv.csv')
    f = open("biorxiv-bioinformatics-cleaned.csv", "w+")
    with open("biorxiv.csv") as fp:
        Lines = fp.readlines()
        current_entry = ''
        for line in Lines:
            # if it is a new whole entry, write the last one to the file and restart the current_entry as line
            if line[0].isnumeric():
                # print(line[0])
                f.write(current_entry + '\n')
                current_entry = line.strip()
            else:
                print('No Num!!')
                # else it is a broken up entry so add it to the last thing
                current_entry += line.strip()
        f.write(current_entry + '\n')
