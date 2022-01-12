using AutoMapper;
using Castle.MicroKernel.Registration;
using Castle.MicroKernel.SubSystems.Configuration;
using Castle.Windsor;
using Microsoft.Extensions.Configuration;

namespace TriosDataLoader
{
    public class AutoMapperInstaller : IWindsorInstaller
    {
        private readonly IConfiguration _config;

        public AutoMapperInstaller(IConfiguration config)
        {
            _config = config;
        }

        public void Install(IWindsorContainer container, IConfigurationStore store)
        {
            container.Register(Component.For<AutoMapper.IMapper>().UsingFactoryMethod(factory => 
                    new MapperConfiguration(map => map.AddProfile<AutoMapperProfile>()).CreateMapper()).LifestyleSingleton()
            );
        }
    }
}