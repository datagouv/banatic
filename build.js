#!/usr/bin/env node
const {createReadStream} = require('fs')
const {createGunzip} = require('zlib')
const bluebird = require('bluebird')
const got = require('got')
const departements = require('@etalab/decoupage-administratif/data/departements.json')
const getStream = require('get-stream').array
const intoStream = require('into-stream').object
const pumpify = require('pumpify').obj
const csvParser = require('csv-parser')
const {flatten, chain} = require('lodash')
const {outputJson} = require('fs-extra')
const Keyv = require('keyv')
const stripBomStream = require('strip-bom-stream')

const cache = new Keyv('sqlite://.cache.sqlite')

const DATE = '01/07/2019'
const HEADERS_COUNT = 145
const MI_BANATIC_API_URL = 'https://www.banatic.interieur.gouv.fr/V5/fichiers-en-telechargement/telecharger.php'

async function loadSirenInseeMapping() {
  const rows = await getStream(pumpify(
    createReadStream('data/correspondance-siren-insee-2019.csv.gz'),
    createGunzip(),
    stripBomStream(),
    csvParser({separator: ';'})
  ))

  return rows.reduce((mapping, row) => {
    mapping[row.siren] = row.insee
    return mapping
  }, {})
}

async function fetchRawData() {
  return flatten(await bluebird.mapSeries(departements, async departement => {
    const cacheKey = `${DATE}-departement-${departement.code}`
    const cacheEntry = await cache.get(cacheKey)
    if (cacheEntry) {
      console.log(`${cacheKey} => HIT`)
      return cacheEntry
    }

    console.log(`${cacheKey} => UPDATING…`)

    const response = await got(`${MI_BANATIC_API_URL}?zone=D${departement.code}&date=${DATE}&format=D`, {encoding: 'latin1', cache})
    const rows = await getStream(pumpify(
      intoStream(response.body),
      csvParser({
        separator: '\t',
        escape: '=@=', // Trick to disable 'escape' behavior
        quote: '=@=' // Trick to disable 'quote' behavior
      })
    ))

    await cache.set(cacheKey, rows)
    return rows
  }))
}

function asDate(value) {
  return (new Date(value)).toISOString().substr(0, 10)
}

function asEnum(value, kv) {
  return kv[value]
}

function asList(row, kv = {}) {
  return Object.keys(kv).reduce((acc, key) => {
    if (row[key] === '1') {
      acc.push(kv[key] === true ? key : kv[key])
    }

    return acc
  }, [])
}

function asCompetences(row) {
  return Object.keys(row).reduce((acc, key) => {
    if (key.match(/^C\d{4}$/) && row[key] === '1') {
      acc.push(key)
    }

    return acc
  }, [])
}

function asString(value) {
  return value.trim() || undefined
}

async function main() {
  const sirenInseeMapping = await loadSirenInseeMapping()
  const rows = await fetchRawData()

  const groupements = chain(rows)
    .filter(row => {
      const isValid = Object.keys(row).length === HEADERS_COUNT
      if (!isValid) {
        console.log('Ligne invalide => ignorée')
      }

      return isValid
    })
    .groupBy('N° SIREN')
    .map((membresRows, siren) => {
      const [gr] = membresRows
      try {
        const groupement = {
          siren,
          nom: gr['Nom du groupement'],
          communeSiege: gr['Commune siège'].substr(0, 9),
          natureJuridique: gr['Nature juridique'],
          dateEffet: asDate(gr['Date d\'effet']),
          modeFinancement: gr['Mode de financement'],
          president: {
            civilite: gr['Civilité Président'],
            prenom: gr['Prénom Président'],
            nom: gr['Nom Président']
          },
          adresseSiege: {
            adresse: [gr['Adresse du siège_1'], gr['Adresse du siège_2'], gr['Adresse du siège_3']].filter(Boolean),
            codePostal: gr['Code postal du siège Ville du siège'].substr(0, 5),
            nomCommuneSiege: gr['Code postal du siège Ville du siège'].substr(6),
            telephone: asString(gr['Téléphone du siège']),
            fax: asString(gr['Fax du siège']),
            courriel: asString(gr['Courriel du siège']),
            siteInternet: asString(gr['Site internet'])
          },
          perceptions: asList(gr, {TEOM: 'taxe-ordures-menageres', REOM: 'redevance-ordures-menageres'}),
          competences: asCompetences(gr),

          membres: chain(membresRows).uniqBy('Siren membre').map(mr => {
            const type = asEnum(mr.Type, {
              Commune: 'commune',
              Groupement: 'groupement',
              'Autre organisme': 'autre'
            })

            const membre = {
              siren: mr['Siren membre'],
              nom: mr['Nom membre'],
              type
            }

            if (type === 'commune') {
              membre.code = sirenInseeMapping[membre.siren]
              if (!membre.code) {
                console.log(`Correspondance SIRENE <=> COG non trouvée pour la commune ${membre.siren}`)
              }
            }

            return membre
          }).value()
        }

        return groupement
      } catch (error) {
        console.error(gr)
        throw error
      }
    })
    .value()

  await outputJson('dist/groupements.json', groupements)
}

main().catch(error => {
  console.error(error)
  process.exit(1)
})
