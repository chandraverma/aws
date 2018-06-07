from mainLogic import main
from sys import version_info
if version_info[0] < 3:
    raise Exception("Python 3 or a more recent version is required.")

if __name__ == "__main__":
    main()
