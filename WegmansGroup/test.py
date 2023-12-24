asa = 1
def question(asa):
    i = 0
    while i < 2:
        answer = input("Address is not matching are you want to continue...(Yes/No) ")
        if any(answer.lower() == f for f in ["yes", 'y', '1', 'ye']):
            print("Yes")
            break
        elif any(answer.lower() == f for f in ['no', 'n', '0']):
            print("No")
            exit()
        else:
            i += 1
            if i < 2:
                print('Please enter yes or no')
            else:
                print("Nothing done")

question(asa)