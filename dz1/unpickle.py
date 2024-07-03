import pickle

if __name__ == '__main__':
    path = 'data/output/Fzg01_40402_1.mf4.pickle'

    try:
        with open(path, 'rb') as handle:
            from_file = pickle.load(handle)
        print(from_file.calculations[2033889808247912781]['min'])
    except Exception as e:
        print(f"Error occurred while loading pickle file: {e}")

   