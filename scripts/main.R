
#
# usethis::create_github_token()

# Token: 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

#
usethis::edit_r_environ()


# GITHUB_PAT= "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"

#
usethis::use_git_remote("origin", url = "https://github.com/EvandroStk9/AlvarasPMSPSMUL.git", 
                        overwrite = TRUE)

#
usethis::use_github(protocol = "https", private = TRUE
                    # auth_token = Sys.getenv("GITHUB_PAT")
                    )

#
# usethis::use_mit_license()


# Importa dados brutos para camada inbound do datalake
source(here::here("scripts", "Alvaras", "alvaras_inbound.R"))
rm(list = ls())
.rs.restartR()

# Faz pré-processamento e flui dados para camada raw do datalake
source(here::here("scripts", "Alvaras", "alvaras_raw.R"))
rm(list = ls())
.rs.restartR()

# Faz pré-processamento e flui dados para camada trusted do datalake
source(here::here("scripts", "Alvaras", "alvaras_trusted.R"))
rm(list = ls())
.rs.restartR()

# Faz georreferenciamento e mantém dados na camada trusted do datalake
source(here::here("scripts", "Alvaras", "alvaras_geoloc.R"))
rm(list = ls())
.rs.restartR()