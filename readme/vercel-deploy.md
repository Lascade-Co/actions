# Vercel Deploy Setup

## GitHub Actions Automated Deployment

This project includes a GitHub Actions workflow that automatically deploys your application based on branch merges or pull request comments:

- Merging to the `staging` branch triggers a deployment to the development environment
- Merging to the `production` branch triggers a deployment to the production environment
- Commenting `.deploy to dev` on a pull request triggers a deployment to the development environment
- Commenting `.deploy to prod` on a pull request triggers a deployment to the production environment

### Setup for GitHub Actions

1. Add the following secrets to your GitHub repository:
   - `VERCEL_DEV_TOKEN`: Your Vercel development token
   - `VERCEL_PROD_TOKEN`: Your Vercel production token

2. The workflow file is located at `.github/workflows/vercel-deploy.yml`

3. You can also manually trigger deployments using the GitHub Actions workflow dispatch feature

### Using Pull Request Comment Deployments

To deploy from a pull request:

1. Open your pull request
2. Add a comment with one of these commands:
   - `.deploy to dev` - Deploys to the development environment
   - `.deploy to prod` - Deploys to the production environment

The GitHub Action will automatically deploy your branch to the specified environment and post a comment with the deployment URL.

## Common Issues and Solutions

### Permission Issues
If you encounter permission issues when removing the `.vercel` directory, the script will automatically try to use `sudo` to remove it.

### Project Settings Error
If you see the error "Could not retrieve Project Settings", the script will automatically remove the `.vercel` directory and retry the deployment.