using Castle.MicroKernel.Registration;
using Castle.MicroKernel.SubSystems.Configuration;
using Castle.Windsor;
using Microsoft.Extensions.Configuration;

namespace TriosDataLoader
{
    public class WindsorInstaller : IWindsorInstaller
    {
        private readonly IConfiguration _configuration;

        public WindsorInstaller(IConfiguration configuration)
        {
            _configuration = configuration;
        }

        public void Install(IWindsorContainer container, IConfigurationStore store)
        {
            container.Register(
                Component.For<IConfiguration>().Instance(_configuration).LifestyleSingleton()
            );
        }
    }
}