KEYS = ['Diesel',
        'Petrol',
        'IT-JOB-VACANCIES',
        'fruit-apples-gala(kg)',
        'it-computing-software',
        'fruit-pears-conference(kg)',
        'vegetable-lettuce-cos(head)',
        'vegetable-tomatoes-plum(kg)',
        'vegetable-cauliflower-all(head)',
        'vegetable-celery-all_washed(kg)',
        'fruit-blueberries-blueberries(kg)',
        'fruit-raspberries-raspberries(kg)',
        'vegetable-asparagus-asparagus(kg)',
        'vegetable-cucumbers-cucumbers(kg)',
        'fruit-strawberries-strawberries(kg)',
        'vegetable-carrots-topped_washed(kg)',
        'vegetable-courgettes-courgettes(kg)',
        'vegetable-sweetcorn-sweetcorn(head)',
        'vegetable-spinach_leaf-loose_bunches(kg)',
        ]

TMP_QUERY = """SELECT *  FROM `datascience-projects.gcp_shareloader.tmpeconomy` 
                        WHERE LABEL IN  ('Diesel', 'Petrol', 'IT-JOB-VACANCIES',
                        'fruit-apples-gala(kg)','it-computing-software',
                        'fruit-pears-conference(kg)',
                        'vegetable-lettuce-cos(head)',
                        'vegetable-tomatoes-plum(kg)',
                        'vegetable-cauliflower-all(head)',
                        'vegetable-celery-all_washed(kg)',
                        'fruit-blueberries-blueberries(kg)',
                        'fruit-raspberries-raspberries(kg)',
                        'vegetable-asparagus-asparagus(kg)',
                        'vegetable-cucumbers-cucumbers(kg)',
                        'fruit-strawberries-strawberries(kg)',
                        'vegetable-carrots-topped_washed(kg)',
                        'vegetable-courgettes-courgettes(kg)',
                        'vegetable-sweetcorn-sweetcorn(head)',
                        'vegetable-spinach_leaf-loose_bunches(kg)'
                        )
                        AND AS_OF_DATE >= DATE(2022,11,1)
                        ORDER BY LABEL, AS_OF_DATE DESC 
                        """