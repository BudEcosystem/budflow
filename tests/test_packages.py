"""Test Community Package Management system."""

import pytest
import asyncio
import json
import tempfile
import zipfile
from unittest.mock import AsyncMock, Mock, patch, MagicMock
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any
from uuid import UUID, uuid4
from pathlib import Path

from budflow.packages.manager import (
    PackageManager,
    Package,
    PackageVersion,
    PackageMetadata,
    PackageDependency,
    PackageRegistry,
    LocalRepository,
    RemoteRepository,
    PackageInstaller,
    PackageValidator,
    NodePackage,
    WorkflowTemplate,
    PackageManifest,
    InstallationStatus,
    VersionRange,
    PackageType,
    PackageStatus,
    PackageError,
    DependencyResolver,
    RegistryClient,
    PackageCache,
    SecurityScanner,
    LicenseValidator,
    PublishRequest,
    SearchQuery,
    SearchResult,
    PackageStats,
    DownloadMetrics,
    ReviewSystem,
    PackageReview,
    RatingScore,
    CompatibilityChecker,
    MigrationManager,
    PackageEvent,
    EventType,
)


@pytest.fixture
def package_manager():
    """Create PackageManager for testing."""
    return PackageManager()


@pytest.fixture
def sample_package():
    """Create sample package for testing."""
    return Package(
        id=uuid4(),
        name="sample-package",
        display_name="Sample Package",
        description="A sample package for testing",
        author="Test Author",
        version="1.0.0",
        package_type=PackageType.NODE,
        keywords=["test", "sample"],
        homepage="https://example.com",
        repository="https://github.com/example/sample",
        license="MIT",
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
        download_count=100,
        rating=4.5,
        review_count=10,
        is_verified=True,
        is_deprecated=False,
        metadata={}
    )


@pytest.fixture
def sample_package_version():
    """Create sample package version."""
    return PackageVersion(
        id=uuid4(),
        package_id=uuid4(),
        version="1.0.0",
        description="Initial release",
        changelog="First version of the package",
        published_at=datetime.now(timezone.utc),
        published_by=uuid4(),
        download_count=50,
        is_prerelease=False,
        is_deprecated=False,
        dependencies=[],
        manifest_url="https://registry.example.com/packages/sample/1.0.0/manifest.json",
        archive_url="https://registry.example.com/packages/sample/1.0.0/package.zip",
        checksum="abc123def456",
        file_size=1024*1024,  # 1MB
        node_compatibility=[">=1.0.0"],
        python_compatibility=[">=3.8"],
        metadata={}
    )


@pytest.fixture
def sample_dependency():
    """Create sample package dependency."""
    return PackageDependency(
        name="dependency-package",
        version_range="^1.0.0",
        is_optional=False,
        environment="production"
    )


@pytest.fixture
def sample_manifest():
    """Create sample package manifest."""
    return PackageManifest(
        name="sample-package",
        version="1.0.0",
        description="A sample package",
        author="Test Author",
        license="MIT",
        main="dist/index.js",
        nodes=["dist/nodes/*.node.js"],
        credentials=["dist/credentials/*.credential.js"],
        webhooks=["dist/webhooks/*.webhook.js"],
        dependencies={
            "lodash": "^4.17.21"
        },
        peer_dependencies={
            "n8n-workflow": ">=0.100.0"
        },
        dev_dependencies={
            "typescript": "^4.0.0"
        },
        scripts={
            "build": "tsc",
            "test": "jest"
        },
        repository="https://github.com/example/sample",
        homepage="https://example.com",
        bugs="https://github.com/example/sample/issues",
        keywords=["automation", "workflow"],
        files=["dist/**/*", "package.json", "README.md"],
        engines={
            "node": ">=14.0.0",
            "n8n": ">=0.100.0"
        },
        metadata={}
    )


@pytest.mark.unit
class TestPackage:
    """Test Package model."""
    
    def test_package_creation(self, sample_package):
        """Test creating package."""
        assert sample_package.name == "sample-package"
        assert sample_package.display_name == "Sample Package"
        assert sample_package.version == "1.0.0"
        assert sample_package.package_type == PackageType.NODE
        assert sample_package.is_verified is True
        assert sample_package.is_deprecated is False
    
    def test_package_serialization(self, sample_package):
        """Test package serialization."""
        data = sample_package.model_dump()
        
        assert "id" in data
        assert "name" in data
        assert "version" in data
        assert "package_type" in data
        
        # Test deserialization
        restored = Package.model_validate(data)
        assert restored.name == sample_package.name
        assert restored.version == sample_package.version
    
    def test_package_slug_generation(self, sample_package):
        """Test package slug generation."""
        slug = sample_package.get_slug()
        assert slug == "sample-package"
        
        # Test with special characters
        sample_package.name = "My Package Name!"
        slug = sample_package.get_slug()
        assert slug == "my-package-name"
    
    def test_package_rating_validation(self, sample_package):
        """Test package rating validation."""
        # Valid ratings
        for rating in [0.0, 2.5, 5.0]:
            sample_package.rating = rating
            assert sample_package.rating == rating
        
        # Test creating packages with invalid ratings (they should be clamped)
        package_low = Package(
            name="test-package",
            display_name="Test Package",
            description="Test package",
            author="Test Author",
            version="1.0.0",
            package_type=PackageType.NODE,
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
            rating=-1.0
        )
        assert package_low.rating >= 0.0
        
        package_high = Package(
            name="test-package",
            display_name="Test Package",
            description="Test package",
            author="Test Author",
            version="1.0.0",
            package_type=PackageType.NODE,
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
            rating=6.0
        )
        assert package_high.rating <= 5.0
    
    def test_package_compatibility_check(self, sample_package):
        """Test package compatibility checking."""
        # Mock version checking
        with patch.object(sample_package, '_check_version_compatibility') as mock_check:
            mock_check.return_value = True
            
            compatible = sample_package.is_compatible_with_version("1.5.0")
            assert compatible is True
            mock_check.assert_called_once_with("1.5.0")


@pytest.mark.unit
class TestPackageVersion:
    """Test PackageVersion model."""
    
    def test_version_creation(self, sample_package_version):
        """Test creating package version."""
        assert sample_package_version.version == "1.0.0"
        assert sample_package_version.is_prerelease is False
        assert sample_package_version.is_deprecated is False
        assert len(sample_package_version.dependencies) == 0
    
    def test_version_semantic_validation(self):
        """Test semantic version validation."""
        # Valid semantic versions
        valid_versions = ["1.0.0", "2.1.3", "0.0.1", "1.0.0-alpha.1", "2.0.0-beta.2"]
        for version in valid_versions:
            pv = PackageVersion(
                package_id=uuid4(),
                version=version,
                description="Test",
                published_at=datetime.now(timezone.utc),
                published_by=uuid4(),
                manifest_url="https://example.com/manifest.json",
                archive_url="https://example.com/package.zip",
                checksum="abc123"
            )
            assert pv.version == version
        
        # Invalid versions should raise ValueError
        invalid_versions = ["1.0", "1", "v1.0.0", ""]
        for version in invalid_versions:
            with pytest.raises(ValueError):
                PackageVersion(
                    package_id=uuid4(),
                    version=version,
                    description="Test",
                    published_at=datetime.now(timezone.utc),
                    published_by=uuid4(),
                    manifest_url="https://example.com/manifest.json",
                    archive_url="https://example.com/package.zip",
                    checksum="abc123"
                )
    
    def test_version_comparison(self, sample_package_version):
        """Test version comparison."""
        newer_version = PackageVersion(
            package_id=sample_package_version.package_id,
            version="2.0.0",
            description="Newer version",
            published_at=datetime.now(timezone.utc),
            published_by=uuid4(),
            manifest_url="https://example.com/manifest.json",
            archive_url="https://example.com/package.zip",
            checksum="def456"
        )
        
        assert sample_package_version.is_newer_than("0.9.0")
        assert not sample_package_version.is_newer_than("1.1.0")
        assert newer_version.is_newer_than(sample_package_version.version)
    
    def test_version_prerelease_handling(self):
        """Test prerelease version handling."""
        prerelease = PackageVersion(
            package_id=uuid4(),
            version="2.0.0-alpha.1",
            description="Prerelease",
            published_at=datetime.now(timezone.utc),
            published_by=uuid4(),
            manifest_url="https://example.com/manifest.json",
            archive_url="https://example.com/package.zip",
            checksum="pre123",
            is_prerelease=True
        )
        
        assert prerelease.is_prerelease is True
        assert prerelease.is_stable() is False


@pytest.mark.unit
class TestPackageDependency:
    """Test PackageDependency model."""
    
    def test_dependency_creation(self, sample_dependency):
        """Test creating package dependency."""
        assert sample_dependency.name == "dependency-package"
        assert sample_dependency.version_range == "^1.0.0"
        assert sample_dependency.is_optional is False
        assert sample_dependency.environment == "production"
    
    def test_dependency_version_matching(self, sample_dependency):
        """Test dependency version matching."""
        # Mock version range checking
        with patch.object(sample_dependency, '_matches_version_range') as mock_match:
            mock_match.return_value = True
            
            matches = sample_dependency.matches_version("1.2.0")
            assert matches is True
            mock_match.assert_called_once_with("1.2.0")
    
    def test_dependency_optional_handling(self):
        """Test optional dependency handling."""
        optional_dep = PackageDependency(
            name="optional-package",
            version_range=">=1.0.0",
            is_optional=True,
            environment="development"
        )
        
        assert optional_dep.is_optional is True
        assert optional_dep.environment == "development"


@pytest.mark.unit
class TestPackageManifest:
    """Test PackageManifest model."""
    
    def test_manifest_creation(self, sample_manifest):
        """Test creating package manifest."""
        assert sample_manifest.name == "sample-package"
        assert sample_manifest.version == "1.0.0"
        assert sample_manifest.author == "Test Author"
        assert "automation" in sample_manifest.keywords
    
    def test_manifest_validation(self, sample_manifest):
        """Test manifest validation."""
        # Valid manifest should pass
        assert sample_manifest.validate() is True
        
        # Missing required fields should fail
        invalid_manifest = PackageManifest(
            name="",  # Empty name should fail
            version="1.0.0",
            description="Test",
            author="Test"
        )
        
        assert invalid_manifest.validate() is False
    
    def test_manifest_dependency_parsing(self, sample_manifest):
        """Test parsing dependencies from manifest."""
        deps = sample_manifest.get_dependencies()
        assert len(deps) > 0
        assert any(dep.name == "lodash" for dep in deps)
    
    def test_manifest_file_inclusion(self, sample_manifest):
        """Test manifest file inclusion patterns."""
        included_files = sample_manifest.get_included_files()
        assert "dist/**/*" in sample_manifest.files
        assert "package.json" in sample_manifest.files
    
    def test_manifest_engine_requirements(self, sample_manifest):
        """Test engine requirements validation."""
        assert "node" in sample_manifest.engines
        assert "n8n" in sample_manifest.engines
        
        node_version = sample_manifest.engines["node"]
        assert node_version == ">=14.0.0"


@pytest.mark.unit
class TestPackageManager:
    """Test PackageManager."""
    
    def test_manager_initialization(self, package_manager):
        """Test manager initialization."""
        assert package_manager is not None
        assert hasattr(package_manager, 'install_package')
        assert hasattr(package_manager, 'uninstall_package')
        assert hasattr(package_manager, 'search_packages')
    
    @pytest.mark.asyncio
    async def test_install_package(self, package_manager, sample_package):
        """Test installing package."""
        with patch.object(package_manager, '_download_package') as mock_download:
            with patch.object(package_manager, '_validate_package') as mock_validate:
                with patch.object(package_manager, '_install_package_files') as mock_install:
                    mock_download.return_value = "/tmp/package.zip"
                    mock_validate.return_value = True
                    mock_install.return_value = ["/path/to/file1", "/path/to/file2"]
                    
                    result = await package_manager.install_package(
                        package_name=sample_package.name,
                        version="1.0.0"
                    )
                    
                    assert result.status == InstallationStatus.SUCCESS
                    assert result.package_name == sample_package.name
                    mock_download.assert_called_once()
                    mock_validate.assert_called_once()
                    mock_install.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_uninstall_package(self, package_manager, sample_package):
        """Test uninstalling package."""
        with patch.object(package_manager, '_is_package_installed') as mock_installed:
            with patch.object(package_manager, '_remove_package_files') as mock_remove:
                with patch.object(package_manager, '_update_registry') as mock_update:
                    mock_installed.return_value = True
                    mock_remove.return_value = True
                    mock_update.return_value = None
                    
                    result = await package_manager.uninstall_package(sample_package.name)
                    
                    assert result is True
                    mock_installed.assert_called_once_with(sample_package.name)
                    mock_remove.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_search_packages(self, package_manager):
        """Test searching packages."""
        search_query = SearchQuery(
            query="sample",
            tags=["test"],
            author="Test Author",
            limit=10,
            offset=0
        )
        
        mock_results = [
            SearchResult(
                package_id=uuid4(),
                name="sample-package",
                display_name="Sample Package",
                description="Test package",
                version="1.0.0",
                author="Test Author",
                rating=4.5,
                download_count=100,
                updated_at=datetime.now(timezone.utc)
            )
        ]
        
        with patch.object(package_manager, '_search_registry') as mock_search:
            mock_search.return_value = mock_results
            
            results = await package_manager.search_packages(search_query)
            
            assert len(results) == 1
            assert results[0].name == "sample-package"
            mock_search.assert_called_once_with(search_query)
    
    @pytest.mark.asyncio
    async def test_update_package(self, package_manager, sample_package):
        """Test updating package to newer version."""
        with patch.object(package_manager, '_get_latest_version') as mock_latest:
            with patch.object(package_manager, 'install_package') as mock_install:
                mock_latest.return_value = "1.1.0"
                mock_install.return_value = Mock(status=InstallationStatus.SUCCESS)
                
                result = await package_manager.update_package(sample_package.name)
                
                assert result.status == InstallationStatus.SUCCESS
                mock_latest.assert_called_once_with(sample_package.name)
                mock_install.assert_called_once_with(sample_package.name, "1.1.0")
    
    @pytest.mark.asyncio
    async def test_list_installed_packages(self, package_manager):
        """Test listing installed packages."""
        mock_packages = [
            Package(
                id=uuid4(),
                name="package-1",
                display_name="Package 1",
                description="First package",
                author="Author 1",
                version="1.0.0",
                package_type=PackageType.NODE,
                created_at=datetime.now(timezone.utc),
                updated_at=datetime.now(timezone.utc)
            ),
            Package(
                id=uuid4(),
                name="package-2",
                display_name="Package 2",
                description="Second package",
                author="Author 2",
                version="2.0.0",
                package_type=PackageType.WORKFLOW,
                created_at=datetime.now(timezone.utc),
                updated_at=datetime.now(timezone.utc)
            )
        ]
        
        with patch.object(package_manager, '_load_installed_packages') as mock_load:
            mock_load.return_value = mock_packages
            
            packages = await package_manager.list_installed_packages()
            
            assert len(packages) == 2
            assert packages[0].name == "package-1"
            assert packages[1].name == "package-2"


@pytest.mark.unit
class TestDependencyResolver:
    """Test DependencyResolver."""
    
    def test_resolver_initialization(self):
        """Test dependency resolver initialization."""
        resolver = DependencyResolver()
        assert resolver is not None
        assert hasattr(resolver, 'resolve_dependencies')
    
    @pytest.mark.asyncio
    async def test_resolve_simple_dependencies(self):
        """Test resolving simple dependency tree."""
        resolver = DependencyResolver()
        
        # Mock package with dependencies
        package_deps = [
            PackageDependency(name="dep1", version_range="^1.0.0"),
            PackageDependency(name="dep2", version_range="^2.0.0")
        ]
        
        with patch.object(resolver, '_fetch_package_info') as mock_fetch:
            mock_fetch.side_effect = [
                Mock(dependencies=[]),  # dep1 has no dependencies
                Mock(dependencies=[])   # dep2 has no dependencies
            ]
            
            resolved = await resolver.resolve_dependencies(package_deps)
            
            assert len(resolved) == 2
            assert any(dep.name == "dep1" for dep in resolved)
            assert any(dep.name == "dep2" for dep in resolved)
    
    @pytest.mark.asyncio
    async def test_resolve_nested_dependencies(self):
        """Test resolving nested dependency tree."""
        resolver = DependencyResolver()
        
        # Package A depends on B, B depends on C
        package_deps = [
            PackageDependency(name="package-b", version_range="^1.0.0")
        ]
        
        with patch.object(resolver, '_fetch_package_info') as mock_fetch:
            mock_fetch.side_effect = [
                Mock(dependencies=[PackageDependency(name="package-c", version_range="^1.0.0")]),  # B depends on C
                Mock(dependencies=[])  # C has no dependencies
            ]
            
            resolved = await resolver.resolve_dependencies(package_deps)
            
            assert len(resolved) >= 2
            assert any(dep.name == "package-b" for dep in resolved)
            assert any(dep.name == "package-c" for dep in resolved)
    
    @pytest.mark.asyncio
    async def test_detect_circular_dependencies(self):
        """Test detecting circular dependencies."""
        resolver = DependencyResolver()
        
        # A depends on B, B depends on A (circular)
        package_deps = [
            PackageDependency(name="package-b", version_range="^1.0.0")
        ]
        
        with patch.object(resolver, '_fetch_package_info') as mock_fetch:
            mock_fetch.side_effect = [
                Mock(dependencies=[PackageDependency(name="package-a", version_range="^1.0.0")]),  # B depends on A
            ]
            
            with pytest.raises(PackageError, match="Circular dependency"):
                await resolver.resolve_dependencies(package_deps, package_name="package-a")


@pytest.mark.unit
class TestPackageValidator:
    """Test PackageValidator."""
    
    def test_validator_initialization(self):
        """Test validator initialization."""
        validator = PackageValidator()
        assert validator is not None
        assert hasattr(validator, 'validate_package')
    
    @pytest.mark.asyncio
    async def test_validate_package_structure(self):
        """Test validating package structure."""
        validator = PackageValidator()
        
        # Create mock package file
        with tempfile.NamedTemporaryFile(suffix='.zip', delete=False) as tmp_file:
            with zipfile.ZipFile(tmp_file.name, 'w') as zip_file:
                zip_file.writestr('package.json', json.dumps({
                    "name": "test-package",
                    "version": "1.0.0",
                    "description": "Test package",
                    "author": "Test Author"
                }))
                zip_file.writestr('dist/nodes/TestNode.node.js', 'module.exports = {};')
            
            result = await validator.validate_package(tmp_file.name)
            assert result.is_valid is True
            assert len(result.errors) == 0
    
    @pytest.mark.asyncio
    async def test_validate_package_manifest(self):
        """Test validating package manifest."""
        validator = PackageValidator()
        
        # Valid manifest
        valid_manifest = {
            "name": "test-package",
            "version": "1.0.0",
            "description": "Test package",
            "author": "Test Author",
            "license": "MIT",
            "main": "dist/index.js"
        }
        
        result = await validator.validate_manifest(valid_manifest)
        assert result.is_valid is True
        
        # Invalid manifest (missing required fields)
        invalid_manifest = {
            "name": "",  # Empty name
            "version": "invalid-version"  # Invalid version
        }
        
        result = await validator.validate_manifest(invalid_manifest)
        assert result.is_valid is False
        assert len(result.errors) > 0
    
    @pytest.mark.asyncio
    async def test_validate_security_scan(self):
        """Test security scanning of package."""
        validator = PackageValidator()
        
        with patch.object(validator, '_run_security_scan') as mock_scan:
            mock_scan.return_value = Mock(
                has_vulnerabilities=False,
                vulnerabilities=[],
                risk_score=0.1
            )
            
            result = await validator.validate_security("/path/to/package.zip")
            
            assert result.is_secure is True
            assert len(result.vulnerabilities) == 0
            mock_scan.assert_called_once()


@pytest.mark.unit
class TestPackageRegistry:
    """Test PackageRegistry."""
    
    def test_registry_initialization(self):
        """Test registry initialization."""
        registry = PackageRegistry()
        assert registry is not None
        assert hasattr(registry, 'publish_package')
        assert hasattr(registry, 'search_packages')
    
    @pytest.mark.asyncio
    async def test_publish_package(self):
        """Test publishing package to registry."""
        registry = PackageRegistry()
        
        publish_request = PublishRequest(
            package_name="test-package",
            version="1.0.0",
            archive_path="/path/to/package.zip",
            manifest={
                "name": "test-package",
                "version": "1.0.0",
                "description": "Test package"
            },
            author_id=uuid4(),
            changelog="Initial release",
            is_prerelease=False
        )
        
        with patch.object(registry, '_upload_package') as mock_upload:
            with patch.object(registry, '_save_package_metadata') as mock_save:
                mock_upload.return_value = "https://registry.example.com/packages/test-package/1.0.0.zip"
                mock_save.return_value = uuid4()
                
                result = await registry.publish_package(publish_request)
                
                assert result.success is True
                assert result.package_id is not None
                mock_upload.assert_called_once()
                mock_save.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_unpublish_package(self):
        """Test unpublishing package from registry."""
        registry = PackageRegistry()
        
        with patch.object(registry, '_remove_package_files') as mock_remove:
            with patch.object(registry, '_update_package_status') as mock_update:
                mock_remove.return_value = True
                mock_update.return_value = None
                
                result = await registry.unpublish_package("test-package", "1.0.0")
                
                assert result is True
                mock_remove.assert_called_once()
                mock_update.assert_called_once()


@pytest.mark.unit
class TestSecurityScanner:
    """Test SecurityScanner."""
    
    def test_scanner_initialization(self):
        """Test security scanner initialization."""
        scanner = SecurityScanner()
        assert scanner is not None
        assert hasattr(scanner, 'scan_package')
    
    @pytest.mark.asyncio
    async def test_scan_package_for_vulnerabilities(self):
        """Test scanning package for vulnerabilities."""
        scanner = SecurityScanner()
        
        with patch.object(scanner, '_scan_dependencies') as mock_scan_deps:
            with patch.object(scanner, '_scan_code') as mock_scan_code:
                with patch.object(scanner, '_scan_permissions') as mock_scan_perms:
                    mock_scan_deps.return_value = []
                    mock_scan_code.return_value = []
                    mock_scan_perms.return_value = []
                    
                    result = await scanner.scan_package("/path/to/package.zip")
                    
                    assert result.is_secure is True
                    assert len(result.vulnerabilities) == 0
                    assert result.risk_score < 0.3  # Low risk
    
    @pytest.mark.asyncio
    async def test_scan_package_with_vulnerabilities(self):
        """Test scanning package with vulnerabilities."""
        scanner = SecurityScanner()
        
        mock_vulnerabilities = [
            {"severity": "HIGH", "description": "SQL Injection vulnerability"},
            {"severity": "MEDIUM", "description": "XSS vulnerability"}
        ]
        
        with patch.object(scanner, '_scan_dependencies') as mock_scan_deps:
            with patch.object(scanner, '_scan_code') as mock_scan_code:
                mock_scan_deps.return_value = mock_vulnerabilities[:1]
                mock_scan_code.return_value = mock_vulnerabilities[1:]
                
                result = await scanner.scan_package("/path/to/package.zip")
                
                assert result.is_secure is False
                assert len(result.vulnerabilities) == 2
                assert result.risk_score > 0.5  # High risk


@pytest.mark.unit
class TestReviewSystem:
    """Test ReviewSystem."""
    
    def test_review_system_initialization(self):
        """Test review system initialization."""
        review_system = ReviewSystem()
        assert review_system is not None
        assert hasattr(review_system, 'submit_review')
        assert hasattr(review_system, 'get_package_reviews')
    
    @pytest.mark.asyncio
    async def test_submit_review(self):
        """Test submitting package review."""
        review_system = ReviewSystem()
        
        review = PackageReview(
            id=uuid4(),
            package_id=uuid4(),
            user_id=uuid4(),
            rating=RatingScore.FIVE_STARS,
            title="Great package!",
            comment="This package works perfectly for my use case.",
            created_at=datetime.now(timezone.utc),
            is_verified_purchase=True
        )
        
        with patch.object(review_system, '_save_review') as mock_save:
            with patch.object(review_system, '_update_package_rating') as mock_update:
                mock_save.return_value = review.id
                mock_update.return_value = None
                
                result = await review_system.submit_review(review)
                
                assert result.success is True
                assert result.review_id == review.id
                mock_save.assert_called_once()
                mock_update.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_package_reviews(self):
        """Test getting package reviews."""
        review_system = ReviewSystem()
        
        mock_reviews = [
            PackageReview(
                id=uuid4(),
                package_id=uuid4(),
                user_id=uuid4(),
                rating=RatingScore.FIVE_STARS,
                title="Excellent",
                comment="Love it!",
                created_at=datetime.now(timezone.utc)
            ),
            PackageReview(
                id=uuid4(),
                package_id=uuid4(),
                user_id=uuid4(),
                rating=RatingScore.FOUR_STARS,
                title="Good",
                comment="Works well",
                created_at=datetime.now(timezone.utc)
            )
        ]
        
        with patch.object(review_system, '_load_reviews') as mock_load:
            mock_load.return_value = mock_reviews
            
            reviews = await review_system.get_package_reviews(uuid4(), limit=10)
            
            assert len(reviews) == 2
            assert reviews[0].rating == RatingScore.FIVE_STARS
            assert reviews[1].rating == RatingScore.FOUR_STARS


@pytest.mark.integration
class TestPackageManagementIntegration:
    """Integration tests for package management."""
    
    @pytest.mark.asyncio
    async def test_full_package_lifecycle(self, package_manager):
        """Test complete package lifecycle."""
        package_name = "test-lifecycle-package"
        
        with patch.object(package_manager, '_download_package'):
            with patch.object(package_manager, '_validate_package'):
                with patch.object(package_manager, '_install_package_files'):
                    with patch.object(package_manager, '_search_registry'):
                        with patch.object(package_manager, '_remove_package_files'):
                            with patch.object(package_manager, '_load_installed_packages') as mock_load:
                                with patch.object(package_manager, '_get_latest_version') as mock_latest:
                                    with patch.object(package_manager, '_is_package_installed') as mock_installed:
                                        with patch.object(package_manager, '_update_registry'):
                                            
                                            # Mock data
                                            mock_package = Package(
                                                name=package_name,
                                                display_name="Test Package",
                                                description="Test package",
                                                author="Test Author",
                                                version="1.0.0",
                                                package_type=PackageType.NODE,
                                                created_at=datetime.now(timezone.utc),
                                                updated_at=datetime.now(timezone.utc)
                                            )
                                            mock_load.return_value = [mock_package]
                                            mock_latest.return_value = "1.1.0"
                                            mock_installed.return_value = True
                                            
                                            # 1. Search for package
                                            search_query = SearchQuery(query=package_name)
                                            await package_manager.search_packages(search_query)
                                            
                                            # 2. Install package
                                            install_result = await package_manager.install_package(package_name, "1.0.0")
                                            assert install_result.status == InstallationStatus.SUCCESS
                                            
                                            # 3. List installed packages
                                            installed = await package_manager.list_installed_packages()
                                            assert any(pkg.name == package_name for pkg in installed)
                                            
                                            # 4. Update package
                                            update_result = await package_manager.update_package(package_name)
                                            assert update_result.status == InstallationStatus.SUCCESS
                                            
                                            # 5. Uninstall package
                                            uninstall_result = await package_manager.uninstall_package(package_name)
                                            assert uninstall_result is True
    
    @pytest.mark.asyncio
    async def test_dependency_resolution_integration(self):
        """Test dependency resolution integration."""
        resolver = DependencyResolver()
        validator = PackageValidator()
        
        # Create mock package with complex dependencies
        main_package_deps = [
            PackageDependency(name="ui-components", version_range="^2.0.0"),
            PackageDependency(name="api-client", version_range="^1.5.0"),
            PackageDependency(name="utils", version_range="^3.0.0")
        ]
        
        with patch.object(resolver, '_fetch_package_info') as mock_fetch:
            with patch.object(validator, 'validate_package') as mock_validate:
                # Mock dependency tree
                mock_fetch.side_effect = [
                    Mock(dependencies=[PackageDependency(name="shared-types", version_range="^1.0.0")]),  # ui-components
                    Mock(dependencies=[PackageDependency(name="http-client", version_range="^2.0.0")]),   # api-client
                    Mock(dependencies=[PackageDependency(name="shared-types", version_range="^1.0.0")]),  # utils
                    Mock(dependencies=[]),  # shared-types
                    Mock(dependencies=[])   # http-client
                ]
                
                mock_validate.return_value = Mock(is_valid=True, errors=[])
                
                # Resolve dependencies
                resolved = await resolver.resolve_dependencies(main_package_deps)
                
                # Should resolve all dependencies including shared ones
                assert len(resolved) >= 5
                dependency_names = [dep.name for dep in resolved]
                assert "ui-components" in dependency_names
                assert "api-client" in dependency_names
                assert "utils" in dependency_names
                assert "shared-types" in dependency_names
                assert "http-client" in dependency_names
    
    @pytest.mark.asyncio
    async def test_security_validation_integration(self):
        """Test security validation integration."""
        validator = PackageValidator()
        scanner = SecurityScanner()
        
        # Test package validation with security scan
        with patch.object(validator, '_run_security_scan') as mock_scan:
            mock_scan.return_value = type('ScanResult', (), {
                'has_vulnerabilities': False,
                'vulnerabilities': [],
                'risk_score': 0.1
            })()
            
            result = await validator.validate_security("/path/to/secure-package.zip")
            
            assert result.is_secure is True
            assert result.risk_score < 0.3
            mock_scan.assert_called_once()


@pytest.mark.performance
class TestPackageManagementPerformance:
    """Performance tests for package management."""
    
    @pytest.mark.asyncio
    async def test_large_package_installation(self, package_manager):
        """Test installing large package performance."""
        # Mock large package (100MB)
        large_package_size = 100 * 1024 * 1024
        
        with patch.object(package_manager, '_download_package') as mock_download:
            with patch.object(package_manager, '_validate_package') as mock_validate:
                with patch.object(package_manager, '_install_package_files') as mock_install:
                    mock_download.return_value = "/tmp/large-package.zip"
                    mock_validate.return_value = True
                    mock_install.return_value = ["/path/to/file1", "/path/to/file2"]
                    
                    import time
                    start_time = time.time()
                    
                    result = await package_manager.install_package(
                        package_name="large-package",
                        version="1.0.0"
                    )
                    
                    end_time = time.time()
                    duration = end_time - start_time
                    
                    # Should complete within reasonable time (< 30 seconds for mock)
                    assert duration < 30.0
                    assert result.status == InstallationStatus.SUCCESS
    
    @pytest.mark.asyncio
    async def test_dependency_resolution_performance(self):
        """Test dependency resolution performance with many dependencies."""
        resolver = DependencyResolver()
        
        # Create package with many dependencies (50 packages)
        many_deps = [
            PackageDependency(name=f"package-{i}", version_range="^1.0.0")
            for i in range(50)
        ]
        
        with patch.object(resolver, '_fetch_package_info') as mock_fetch:
            # Each package has no further dependencies
            mock_fetch.return_value = Mock(dependencies=[])
            
            import time
            start_time = time.time()
            
            resolved = await resolver.resolve_dependencies(many_deps)
            
            end_time = time.time()
            duration = end_time - start_time
            
            # Should resolve quickly (< 5 seconds)
            assert duration < 5.0
            assert len(resolved) == 50
    
    @pytest.mark.asyncio
    async def test_package_search_performance(self, package_manager):
        """Test package search performance."""
        # Mock large search result set
        large_result_set = [
            SearchResult(
                package_id=uuid4(),
                name=f"package-{i}",
                display_name=f"Package {i}",
                description=f"Description for package {i}",
                version="1.0.0",
                author="Test Author",
                rating=4.0,
                download_count=100,
                updated_at=datetime.now(timezone.utc)
            )
            for i in range(1000)
        ]
        
        with patch.object(package_manager, '_search_registry') as mock_search:
            mock_search.return_value = large_result_set
            
            search_query = SearchQuery(query="package", limit=1000)
            
            import time
            start_time = time.time()
            
            results = await package_manager.search_packages(search_query)
            
            end_time = time.time()
            duration = end_time - start_time
            
            # Should search quickly (< 2 seconds)
            assert duration < 2.0
            assert len(results) == 1000