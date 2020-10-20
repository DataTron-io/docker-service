

import json





def fix_structure(input):
    output = []

    output.append(input["input"]["Black"])
    output.append(input["input"]["status"]["Married"])
    output.append(input["input"]["status"]["Boy"])
    output.append(input["input"]["status"]["MomAge"])
    output.append(input["input"]["status"]["MomSmoke"])
    output.append(input["input"]["status"]["CigsPerDay"])
    output.append(input["input"]["status"]["MomWtGain"])
    output.append(input["Visit"])
    output.append(input["Test"]["MomEdLevel"])

    return output
