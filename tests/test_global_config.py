from dbx_tester.global_config import GlobalConfigManager

def test_global_config_manager():
    GlobalConfigManager().add_config(test_path="C://Users//vignesh.manoharan//OneDrive - Perficient, Inc//Desktop//projects//dbx_tester//dbx_tester//tests",
                                     cluster_id= "12345")

if __name__ == '__main__':
    test_global_config_manager()