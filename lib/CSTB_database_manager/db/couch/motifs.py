import json, os

class MotifsDB():
    def __init__(self, wrapper, mapping_rules):
        self.wrapper = wrapper
        if not os.path.isfile(mapping_rules):
            raise Exception(f"{mapping_rules} file doesn't exists")
        self.volumes_list = self._volumes_list(mapping_rules)

    def _volumes_list(self, mapping_rules):
        with open(mapping_rules) as f:
            mapping_dict = json.load(f)
        return list(mapping_dict.values())

    @property
    def entries_per_volume(self):
        result_dic = {}
        for v in self.volumes_list:
            answer = self.wrapper.couchGetRequest(v)
            if "error" in answer:
                raise Exception(f"error when try to interrogate {v} : {answer}")
            
            if not "doc_count" in answer:
                raise Exception(f"Can't have doc count for {v} : {answer}")

            result_dic[v] = answer["doc_count"]

        return result_dic
            

         
