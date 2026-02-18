# Apply/destroy cycle test - no cloud credentials, no filesystem writes
# Regression test for issue #2: map(string) coercion of integer values

terraform {
  required_providers {
    random = {
      source  = "hashicorp/random"
      version = "3.7.2"
    }
    null = {
      source  = "hashicorp/null"
      version = "3.2.4"
    }
  }
}

provider "random" {}
provider "null" {}

resource "random_pet" "name" {
  length    = 2
  separator = "-"
}

resource "random_integer" "port" {
  min = 8000
  max = 9000
}

# Regression: triggers is map(string) but random_integer.port.result is an integer.
# Without scalar coercion this fails with msgpack type mismatch.
resource "null_resource" "coerce_test" {
  triggers = {
    pet_name = random_pet.name.id
    port     = random_integer.port.result
  }
}

output "pet_name" {
  value = random_pet.name.id
}

output "port" {
  value = random_integer.port.result
}
