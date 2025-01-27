from selenium.webdriver.common.by import By

import local_common as common
import dbwriter
from pathlib import Path

from prefect import task, Flow
from prefect.cache_policies import NO_CACHE

@task(cache_policy=NO_CACHE)
def set_up_driver():
    return common.set_up_driver()

@task(cache_policy=NO_CACHE)
def connect(DRIVER):
    print("starting...")
    web = "https://www.winterparkresort.com/the-mountain/mountain-report#lift-and-trail-status"
    DRIVER.get(web)

    # LIFTS
    rows = DRIVER.find_elements(By.XPATH, '//li[contains(@class, "Lift")]')
    lifts = []
    for row in rows:
        try:
            lift_name = common.safeSearch(row, By.XPATH, './/p[contains(@class, "Lift_name")]').get_attribute("innerHTML")
            
            # STATUS
            lift_status = False
            if common.isElementPresent(row, By.XPATH, './/*[name()="svg"][contains(@data-src, "open")]'):
                lift_status = True
            
            # CHAIRTYPE
            if common.isElementPresent(row, By.XPATH, './/*[name()="svg"][contains(@data-src, "cabriolet")]'):
                lift_type = "Cabriolet"
            elif common.isElementPresent(row, By.XPATH, './/*[name()="svg"][contains(@data-src, "magic_carpet")]'):
                lift_type = "Magic Carpet"
            elif common.isElementPresent(row, By.XPATH, './/*[name()="svg"][contains(@data-src, "double")]'):
                lift_type = "Double"
            elif common.isElementPresent(row, By.XPATH, './/*[name()="svg"][contains(@data-src, "triple")]'):
                lift_type = "Triple"
            elif common.isElementPresent(row, By.XPATH, './/*[name()="svg"][contains(@data-src, "quad")]'):
                lift_type = "Quad"
            elif common.isElementPresent(row, By.XPATH, './/*[name()="svg"][contains(@data-src, "six")]'):
                lift_type = "Six"
            elif common.isElementPresent(row, By.XPATH, './/*[name()="svg"][contains(@data-src, "rope_tow")]'):
                lift_type = "Tow Rope"
            else:
                continue
            
            lift_obj = {
                "liftName": lift_name,
                "liftType": lift_type,
                "liftStatus": lift_status,
            }
            # print(lift_obj)
            lifts.append(lift_obj)
        except Exception as e:
            print("skipping row")

    # print(lifts)
    # RUNS
    rows = DRIVER.find_elements(By.XPATH, '//li[contains(@class, "TrailWidget")]')
    runs = []
    for row in rows:
        try:
            run_name = common.safeSearch(row, By.XPATH, './/p[contains(@class, "name")]').get_attribute("innerHTML")
            
            run_status = False
            if "open" in common.safeSearch(row, By.XPATH, './/p[contains(@class, "status")]').get_attribute("innerHTML").lower():
                run_status = True
            
            # GROOMED
            run_groomed = False
            if common.isElementPresent(row, By.XPATH, './/*[name()="svg"][contains(@data-src, "grooming")]'):
                run_groomed = True

            # DIFFICULTY
            if common.isElementPresent(row, By.XPATH, './/*[name()="svg"][contains(@data-src, "green-circle")]'):
                run_difficulty = "green"
            elif common.isElementPresent(row, By.XPATH, './/*[name()="svg"][contains(@data-src, "blue-square")]'):
                run_difficulty = "blue"
            elif common.isElementPresent(row, By.XPATH, './/*[name()="svg"][contains(@data-src, "blue-black-square")]'):
                run_difficulty = "blue2"
            elif common.isElementPresent(row, By.XPATH, './/*[name()="svg"][contains(@data-src, "black-diamond")]'):
                run_difficulty = "black1"
            elif common.isElementPresent(row, By.XPATH, './/*[name()="svg"][contains(@data-src, "double-black-diamond")]'):
                run_difficulty = "black2"
            elif common.isElementPresent(row, By.XPATH, './/*[name()="svg"][contains(@data-src, "park")]'):
                run_difficulty = "terrainpark"
            else:
                print("AA")
                continue
            
            run_obj = {
                "runName": run_name,
                "runStatus": run_status,
                "runDifficulty": run_difficulty,
                "runGroomed": run_groomed
            }
            # print(run_obj)
            runs.append(run_obj)
        except Exception as e:
            print("skipping row")
    
    return lifts, runs

@task
def export(lifts, runs, location):
    json_string = common.prepareForExport(lifts, runs, location)
    results = dbwriter.main(json_string)
    return results

@Flow
def main():
    DRIVER = set_up_driver()
    lifts, runs = connect(DRIVER)
    results = export(lifts, runs, "winterpark")
    print(results)
    print("Done")

if __name__ == "__main__":
    main()
    main.from_source(
        source=str(Path(__file__).parent),
        entrypoint="winterpark.py:main",
    ).deploy(
        name="corduroy-scraper-winterpark",
        work_pool_name="local-pool",
        cron="0 13 * 10-12,1-5 *"
    )