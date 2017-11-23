import fs from "fs"
import path from "path"
import stream from "stream"
import { getDatasetPath, SkipFirstLine, ColumnExtractor } from "../helpers.mjs"
import { TRAFFIC_ACCIDENTS } from "../datasets"

const annualCounts = new Map()

fs.createReadStream(getDatasetPath(TRAFFIC_ACCIDENTS))
  .pipe(new SkipFirstLine)
  .pipe(new ColumnExtractor([7]))
  .on(`data`, (data) => {
    const year = JSON.parse(data.toString())[0].slice(0, 4)
    if (!annualCounts.has(year)) {
      annualCounts.set(year, 0)
    }
    annualCounts.set(year, annualCounts.get(year) + 1)
  })
  .on(`end`, () => console.log(annualCounts))
  .pipe(fs.createWriteStream(path.resolve(`./annualCounts.txt`)))