

import json





def fix_structure(input):
    output = []

    output.append(input["input"]["Black"])
    output.append(input["input"]["status"]["Married"])
    output.append(input["input"]["status"]["info"]["Boy"])
    output.append(input["input"]["status"]["info"]["MomAge"])
    output.append(input["input"]["status"]["info"]["MomSmoke"])
    output.append(input["input"]["status"]["CigsPerDay"])
    output.append(input["input"]["status"]["MomWtGain"])
    output.append(input["Visit"])
    output.append(input["NestOfNests"]["Nest2"]["Nest3"]["Nest4"]["MomEdLevel"])

    return output
